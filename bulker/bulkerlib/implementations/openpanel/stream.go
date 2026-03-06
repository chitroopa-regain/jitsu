package openpanel

import (
	"context"
	"fmt"
	"time"

	bulkerlib "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/jsonorder"
)

type OpenPanelStream struct {
	id      string
	bulker  *OpenPanelBulker
	options bulkerlib.StreamOptions

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

func (s *OpenPanelStream) consumeMap(msg map[string]any) (bulkerlib.State, types.Object, error) {
	s.state.ProcessedRows++

	msgType, _ := msg["type"].(string)

	switch msgType {
	case "track", "screen":
		event, ip := MapEvent(msg, s.bulker.config.ProjectID)
		s.bulker.geoEnrich.Enrich(event, ip)

		var syntheticEvents []map[string]any
		s.bulker.sessionMgr.ProcessEvent(event, &s.sessionsBuf, &syntheticEvents)

		s.eventsBuf = append(s.eventsBuf, event)
		s.eventsBuf = append(s.eventsBuf, syntheticEvents...)

		country, _ := event["country"].(string)
		s.bulker.profileMgr.EnsureProfile(msg, country, &s.profilesBuf)

	case "identify":
		s.bulker.profileMgr.ProcessIdentify(msg, &s.profilesBuf, &s.aliasesBuf)

	case "alias":
		s.bulker.profileMgr.ProcessAlias(msg, &s.aliasesBuf)
	}

	s.state.SuccessfulRows++
	return s.state, nil, nil
}

func (s *OpenPanelStream) Abort(ctx context.Context) bulkerlib.State {
	s.state.Status = bulkerlib.Aborted
	s.eventsBuf = nil
	s.sessionsBuf = nil
	s.profilesBuf = nil
	s.aliasesBuf = nil
	return s.state
}

func (s *OpenPanelStream) Complete(ctx context.Context) (bulkerlib.State, error) {
	if s.state.Status != bulkerlib.Active {
		return s.state, fmt.Errorf("stream is not active")
	}

	db := s.bulker.config.Database

	// Write all 4 tables
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
