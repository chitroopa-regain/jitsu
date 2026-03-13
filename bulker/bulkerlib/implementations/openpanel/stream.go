package openpanel

import (
	"context"
	"fmt"
	"time"

	bulkerlib "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/jsonorder"
	"github.com/jitsucom/bulker/jitsubase/logging"
)

// profileInput holds minimal fields extracted during consumeMap for batch profile processing.
type profileInput struct {
	profileID   string
	userID      string
	anonymousID string
	context     map[string]any // for extractDeviceProps()
	traits      map[string]any // only populated for identify events
	timestamp   string
}

// aliasInput holds minimal fields extracted during consumeMap for alias processing.
type aliasInput struct {
	userID     string
	previousID string
	timestamp  string
}

type OpenPanelStream struct {
	id      string
	bulker  *OpenPanelBulker
	options bulkerlib.StreamOptions

	// Populated during consumeMap (CPU-only work):
	trackEvents   []map[string]any // output of MapEvent() for track/screen
	trackIPs      []string         // extracted IPs for geo batch
	trackProfiles []profileInput   // minimal fields for EnsureProfilesBatch

	identifyInputs []profileInput // minimal fields for ProcessIdentifyBatch
	aliasInputs    []aliasInput   // minimal fields for alias processing

	// Populated during Complete (batch I/O):
	eventsBuf   []map[string]any
	sessionsBuf []map[string]any
	profilesBuf []map[string]any
	aliasesBuf  []map[string]any

	state     bulkerlib.State
	startTime time.Time
}

func NewOpenPanelStream(id string, b *OpenPanelBulker, streamOptions ...bulkerlib.StreamOption) *OpenPanelStream {
	s := &OpenPanelStream{
		id:        id,
		bulker:    b,
		startTime: time.Now(),
	}
	s.options = bulkerlib.StreamOptions{}
	for _, opt := range streamOptions {
		s.options.Add(opt)
	}
	s.state = bulkerlib.State{
		Status: bulkerlib.Active,
		Representation: map[string]string{
			"name": OpenPanelBulkerTypeId,
		},
	}
	return s
}

func (s *OpenPanelStream) Consume(ctx context.Context, object types.Object) (bulkerlib.State, types.Object, error) {
	m := types.ObjectToMap(object)
	return s.consumeMap(m)
}

func (s *OpenPanelStream) ConsumeJSON(ctx context.Context, jsonBytes []byte) (bulkerlib.State, types.Object, error) {
	var obj types.Object
	if err := jsonorder.Unmarshal(jsonBytes, &obj); err != nil {
		return s.state, nil, fmt.Errorf("error parsing JSON: %v", err)
	}
	return s.Consume(ctx, obj)
}

func (s *OpenPanelStream) ConsumeMap(ctx context.Context, mp map[string]any) (bulkerlib.State, types.Object, error) {
	return s.consumeMap(mp)
}

// consumeMap does CPU-only work: parses and classifies the message, extracts minimal
// fields needed for batch I/O, and buffers them. The full raw msg is GC'd after return.
func (s *OpenPanelStream) consumeMap(msg map[string]any) (bulkerlib.State, types.Object, error) {
	s.state.ProcessedRows++

	msgType, _ := msg["type"].(string)

	switch msgType {
	case "track", "screen":
		// MapEvent is CPU-only (field mapping, no I/O)
		event, ip := MapEvent(msg, s.bulker.config.ProjectID)
		s.trackEvents = append(s.trackEvents, event)
		s.trackIPs = append(s.trackIPs, ip)

		// Extract minimal fields for profile processing
		s.trackProfiles = append(s.trackProfiles, profileInput{
			profileID:   firstNonEmpty(msg, "userId", "user_id", "anonymousId", "anonymous_id"),
			userID:      firstNonEmpty(msg, "userId", "user_id"),
			anonymousID: firstNonEmpty(msg, "anonymousId", "anonymous_id"),
			context:     getMap(msg, "context"),
			timestamp:   getString(msg, "timestamp", time.Now().UTC().Format(time.RFC3339Nano)),
		})

	case "identify":
		s.identifyInputs = append(s.identifyInputs, profileInput{
			profileID:   firstNonEmpty(msg, "userId", "user_id", "anonymousId", "anonymous_id"),
			userID:      firstNonEmpty(msg, "userId", "user_id"),
			anonymousID: firstNonEmpty(msg, "anonymousId", "anonymous_id"),
			context:     getMap(msg, "context"),
			traits:      getMap(msg, "traits"),
			timestamp:   getString(msg, "timestamp", time.Now().UTC().Format(time.RFC3339Nano)),
		})

	case "alias":
		s.aliasInputs = append(s.aliasInputs, aliasInput{
			userID:     firstNonEmpty(msg, "userId", "user_id"),
			previousID: firstNonEmpty(msg, "previousId", "previous_id", "anonymousId", "anonymous_id"),
			timestamp:  getString(msg, "timestamp", time.Now().UTC().Format(time.RFC3339Nano)),
		})

	default:
		logging.Warnf("[openpanel] unknown message type %q, skipping", msgType)
	}

	s.state.SuccessfulRows++
	return s.state, nil, nil
}

func (s *OpenPanelStream) Abort(ctx context.Context) bulkerlib.State {
	s.state.Status = bulkerlib.Aborted
	s.trackEvents = nil
	s.trackIPs = nil
	s.trackProfiles = nil
	s.identifyInputs = nil
	s.aliasInputs = nil
	s.eventsBuf = nil
	s.sessionsBuf = nil
	s.profilesBuf = nil
	s.aliasesBuf = nil
	return s.state
}

// Complete performs all batch I/O: geo enrichment, session processing, profile processing,
// then writes to ClickHouse.
func (s *OpenPanelStream) Complete(ctx context.Context) (bulkerlib.State, error) {
	if s.state.Status != bulkerlib.Active {
		return s.state, fmt.Errorf("stream is not active")
	}

	// Phase 1: Batch geo enrichment (single POST to Gunter batch endpoint)
	if err := s.bulker.geoEnrich.EnrichBatch(s.trackEvents, s.trackIPs); err != nil {
		s.state.SetError(err)
		s.state.Status = bulkerlib.Failed
		return s.state, fmt.Errorf("geo enrichment failed (Kafka will retry): %w", err)
	}

	// Phase 2: Batch session processing (2 Redis round-trips via Pipeline)
	if err := s.bulker.sessionMgr.ProcessEventsBatch(s.trackEvents, &s.sessionsBuf, &s.eventsBuf); err != nil {
		s.state.SetError(err)
		s.state.Status = bulkerlib.Failed
		return s.state, fmt.Errorf("session batch failed (Kafka will retry): %w", err)
	}
	// trackEvents themselves are the real events; eventsBuf now has synthetic events
	s.eventsBuf = append(s.eventsBuf, s.trackEvents...)

	// Phase 3: Batch profile processing (2-4 Redis round-trips via Pipeline)
	// Process identify events FIRST so their richer profiles are in Redis before
	// EnsureProfilesBatch runs — otherwise default track profiles could overwrite
	// identify traits in ClickHouse's ReplacingMergeTree.
	identifiedIDs, err := s.bulker.profileMgr.ProcessIdentifyBatch(s.identifyInputs, &s.profilesBuf, &s.aliasesBuf)
	if err != nil {
		s.state.SetError(err)
		s.state.Status = bulkerlib.Failed
		return s.state, fmt.Errorf("identify batch failed (Kafka will retry): %w", err)
	}
	if err := s.bulker.profileMgr.EnsureProfilesBatch(s.trackProfiles, s.trackEvents, &s.profilesBuf, identifiedIDs); err != nil {
		s.state.SetError(err)
		s.state.Status = bulkerlib.Failed
		return s.state, fmt.Errorf("ensure profiles batch failed (Kafka will retry): %w", err)
	}

	// Process alias events (no I/O needed)
	for _, a := range s.aliasInputs {
		if a.userID != "" && a.previousID != "" {
			s.aliasesBuf = append(s.aliasesBuf, map[string]any{
				"project_id": s.bulker.config.ProjectID,
				"profile_id": a.userID,
				"alias":      a.previousID,
				"created_at": a.timestamp,
			})
		}
	}

	// Phase 4: Write all 4 tables to ClickHouse
	db := s.bulker.config.Database

	if err := WriteEvents(ctx, s.bulker.chConn, db, s.eventsBuf); err != nil {
		s.state.SetError(err)
		s.state.Status = bulkerlib.Failed
		return s.state, err
	}
	if err := WriteSessions(ctx, s.bulker.chConn, db, s.sessionsBuf); err != nil {
		s.state.SetError(err)
		s.state.Status = bulkerlib.Failed
		return s.state, err
	}
	if err := WriteProfiles(ctx, s.bulker.chConn, db, s.profilesBuf); err != nil {
		s.state.SetError(err)
		s.state.Status = bulkerlib.Failed
		return s.state, err
	}
	if err := WriteAliases(ctx, s.bulker.chConn, db, s.aliasesBuf); err != nil {
		s.state.SetError(err)
		s.state.Status = bulkerlib.Failed
		return s.state, err
	}

	s.state.Status = bulkerlib.Completed
	s.state.ProcessingTimeSec = time.Since(s.startTime).Seconds()
	return s.state, nil
}
