package openpanel

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	bulkerlib "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/redis/go-redis/v9"
)

const OpenPanelBulkerTypeId = "openpanel"

func init() {
	bulkerlib.RegisterBulker(OpenPanelBulkerTypeId, NewOpenPanelBulker)
}

type OpenPanelConfig struct {
	// ClickHouse connection
	Protocol   string `mapstructure:"protocol" json:"protocol"`
	Hosts      string `mapstructure:"hosts" json:"hosts"`
	Database   string `mapstructure:"database" json:"database"`
	Username   string `mapstructure:"username" json:"username"`
	Password   string `mapstructure:"password" json:"password"`
	Parameters string `mapstructure:"parameters" json:"parameters"`
	SSLEnabled bool   `mapstructure:"ssl" json:"ssl"`

	// OpenPanel-specific
	ProjectID     string `mapstructure:"projectId" json:"projectId"`
	GeoServiceURL string `mapstructure:"geoServiceUrl" json:"geoServiceUrl"`
	RedisURL      string `mapstructure:"redisUrl" json:"redisUrl"`
}

type OpenPanelBulker struct {
	appbase.Service
	config     OpenPanelConfig
	chConn     clickhouse.Conn
	rdb        *redis.Client
	geoEnrich  *GeoEnricher
	sessionMgr *SessionManager
	profileMgr *ProfileManager
	closed     *atomic.Bool
}

func NewOpenPanelBulker(bulkerConfig bulkerlib.Config) (bulkerlib.Bulker, error) {
	cfg := OpenPanelConfig{}
	if err := utils.ParseObject(bulkerConfig.DestinationConfig, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse openpanel config: %v", err)
	}

	if cfg.Database == "" {
		cfg.Database = "openpanel"
	}
	if cfg.Username == "" {
		cfg.Username = "default"
	}
	if cfg.ProjectID == "" {
		cfg.ProjectID = "regain-app"
	}

	// Connect to ClickHouse
	chOpts := &clickhouse.Options{
		Addr: []string{cfg.Hosts},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 10 * time.Second,
	}
	if cfg.SSLEnabled {
		chOpts.Protocol = clickhouse.Native
	}

	chConn, err := clickhouse.Open(chOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to clickhouse: %v", err)
	}
	if err := chConn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping clickhouse: %v", err)
	}

	// Connect to Redis
	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse redis URL: %v", err)
	}
	rdb := redis.NewClient(redisOpts)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %v", err)
	}

	geoEnrich := NewGeoEnricher(cfg.GeoServiceURL)
	sessionMgr := NewSessionManager(rdb, cfg.ProjectID)
	profileMgr := NewProfileManager(rdb, cfg.ProjectID)

	return &OpenPanelBulker{
		Service:    appbase.NewServiceBase(OpenPanelBulkerTypeId),
		config:     cfg,
		chConn:     chConn,
		rdb:        rdb,
		geoEnrich:  geoEnrich,
		sessionMgr: sessionMgr,
		profileMgr: profileMgr,
		closed:     &atomic.Bool{},
	}, nil
}

func (b *OpenPanelBulker) CreateStream(id, tableName string, mode bulkerlib.BulkMode, streamOptions ...bulkerlib.StreamOption) (bulkerlib.BulkerStream, error) {
	switch mode {
	case bulkerlib.Batch, bulkerlib.ReplaceTable:
		return NewOpenPanelStream(id, b, streamOptions...), nil
	default:
		return nil, fmt.Errorf("openpanel destination only supports batch mode, got: %s", mode)
	}
}

func (b *OpenPanelBulker) Type() string {
	return OpenPanelBulkerTypeId
}

func (b *OpenPanelBulker) Close() error {
	b.closed.Store(true)
	b.rdb.Close()
	return b.chConn.Close()
}
