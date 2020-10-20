package tenant

import (
	"context"
	"encoding/json"
	"time"

	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kv"
)

const (
	orgOperationLogKeyPrefix    = "org"
	bucketOperationLogKeyPrefix = "bucket"
	userOperationLogKeyPrefix   = "user"

	organizationCreatedEvent = "Organization Created"
	organizationUpdatedEvent = "Organization Updated"
	bucketCreatedEvent       = "Bucket Created"
	bucketUpdatedEvent       = "Bucket Updated"
	userCreatedEvent         = "User Created"
	userUpdatedEvent         = "User Updated"
)

// OpLogStore is a type which persists and reports operation log entries on a backing
// kv store transaction.
type OpLogStore interface {
	AddLogEntryTx(ctx context.Context, tx kv.Tx, k, v []byte, t time.Time) error
	ForEachLogEntryTx(ctx context.Context, tx kv.Tx, k []byte, opts influxdb.FindOptions, fn func([]byte, time.Time) error) error
}

// OpLogService is a type which stores operation logs for buckets, users and orgs.
type OpLogService struct {
	kv kv.Store

	opLogStore OpLogStore

	TimeGenerator influxdb.TimeGenerator
}

// NewOpLogService constructs and configures a new op log service.
func NewOpLogService(store kv.Store, opLogStore OpLogStore) *OpLogService {
	return &OpLogService{
		kv:            store,
		opLogStore:    opLogStore,
		TimeGenerator: influxdb.RealTimeGenerator{},
	}
}

// GetOrganizationOperationLog retrieves a organization operation log.
func (s *OpLogService) GetOrganizationOperationLog(ctx context.Context, id influxdb.ID, opts influxdb.FindOptions) ([]*influxdb.OperationLogEntry, int, error) {
	// TODO(desa): might be worthwhile to allocate a slice of size opts.Limit
	log := []*influxdb.OperationLogEntry{}

	err := s.kv.View(ctx, func(tx kv.Tx) error {
		key, err := encodeOrganizationOperationLogKey(id)
		if err != nil {
			return err
		}

		return s.opLogStore.ForEachLogEntryTx(ctx, tx, key, opts, func(v []byte, t time.Time) error {
			e := &influxdb.OperationLogEntry{}
			if err := json.Unmarshal(v, e); err != nil {
				return err
			}
			e.Time = t

			log = append(log, e)

			return nil
		})
	})

	if err != nil && err != kv.ErrKeyValueLogBoundsNotFound {
		return nil, 0, err
	}

	return log, len(log), nil
}

// GetBucketOperationLog retrieves a buckets operation log.
func (s *OpLogService) GetBucketOperationLog(ctx context.Context, id influxdb.ID, opts influxdb.FindOptions) ([]*influxdb.OperationLogEntry, int, error) {
	// TODO(desa): might be worthwhile to allocate a slice of size opts.Limit
	log := []*influxdb.OperationLogEntry{}

	err := s.kv.View(ctx, func(tx kv.Tx) error {
		key, err := encodeBucketOperationLogKey(id)
		if err != nil {
			return err
		}

		return s.opLogStore.ForEachLogEntryTx(ctx, tx, key, opts, func(v []byte, t time.Time) error {
			e := &influxdb.OperationLogEntry{}
			if err := json.Unmarshal(v, e); err != nil {
				return err
			}
			e.Time = t

			log = append(log, e)

			return nil
		})
	})

	if err != nil && err != kv.ErrKeyValueLogBoundsNotFound {
		return nil, 0, err
	}

	return log, len(log), nil
}

// GetUserOperationLog retrieves a user operation log.
func (s *OpLogService) GetUserOperationLog(ctx context.Context, id influxdb.ID, opts influxdb.FindOptions) ([]*influxdb.OperationLogEntry, int, error) {
	// TODO(desa): might be worthwhile to allocate a slice of size opts.Limit
	log := []*influxdb.OperationLogEntry{}

	err := s.kv.View(ctx, func(tx kv.Tx) error {
		key, err := encodeUserOperationLogKey(id)
		if err != nil {
			return err
		}

		return s.opLogStore.ForEachLogEntryTx(ctx, tx, key, opts, func(v []byte, t time.Time) error {
			e := &influxdb.OperationLogEntry{}
			if err := json.Unmarshal(v, e); err != nil {
				return err
			}
			e.Time = t

			log = append(log, e)

			return nil
		})
	})

	if err != nil && err != kv.ErrKeyValueLogBoundsNotFound {
		return nil, 0, err
	}

	return log, len(log), nil
}

func (s *OpLogService) appendOrganizationEventToLog(ctx context.Context, tx kv.Tx, id influxdb.ID, st string) error {
	e := &influxdb.OperationLogEntry{
		Description: st,
	}
	// TODO(desa): this is fragile and non explicit since it requires an authorizer to be on context. It should be
	//             replaced with a higher level transaction so that adding to the log can take place in the http handler
	//             where the organizationID will exist explicitly.
	a, err := icontext.GetAuthorizer(ctx)
	if err == nil {
		// Add the organization to the log if you can, but don't error if its not there.
		e.UserID = a.GetUserID()
	}

	v, err := json.Marshal(e)
	if err != nil {
		return err
	}

	k, err := encodeOrganizationOperationLogKey(id)
	if err != nil {
		return err
	}

	return s.opLogStore.AddLogEntryTx(ctx, tx, k, v, s.TimeGenerator.Now())
}

func (s *OpLogService) appendBucketEventToLog(ctx context.Context, tx kv.Tx, id influxdb.ID, st string) error {
	e := &influxdb.OperationLogEntry{
		Description: st,
	}
	// TODO(desa): this is fragile and non explicit since it requires an authorizer to be on context. It should be
	//             replaced with a higher level transaction so that adding to the log can take place in the http handler
	//             where the userID will exist explicitly.
	a, err := icontext.GetAuthorizer(ctx)
	if err == nil {
		// Add the user to the log if you can, but don't error if its not there.
		e.UserID = a.GetUserID()
	}

	v, err := json.Marshal(e)
	if err != nil {
		return err
	}

	k, err := encodeBucketOperationLogKey(id)
	if err != nil {
		return err
	}

	return s.opLogStore.AddLogEntryTx(ctx, tx, k, v, s.TimeGenerator.Now())
}

func (s *OpLogService) appendUserEventToLog(ctx context.Context, tx kv.Tx, id influxdb.ID, st string) error {
	e := &influxdb.OperationLogEntry{
		Description: st,
	}
	// TODO(desa): this is fragile and non explicit since it requires an authorizer to be on context. It should be
	//             replaced with a higher level transaction so that adding to the log can take place in the http handler
	//             where the userID will exist explicitly.
	a, err := icontext.GetAuthorizer(ctx)
	if err == nil {
		// Add the user to the log if you can, but don't error if its not there.
		e.UserID = a.GetUserID()
	}

	v, err := json.Marshal(e)
	if err != nil {
		return err
	}

	k, err := encodeUserOperationLogKey(id)
	if err != nil {
		return err
	}

	return s.opLogStore.AddLogEntryTx(ctx, tx, k, v, s.TimeGenerator.Now())
}

func encodeOrganizationOperationLogKey(id influxdb.ID) ([]byte, error) {
	buf, err := id.Encode()
	if err != nil {
		return nil, err
	}
	return append([]byte(orgOperationLogKeyPrefix), buf...), nil
}

func encodeBucketOperationLogKey(id influxdb.ID) ([]byte, error) {
	buf, err := id.Encode()
	if err != nil {
		return nil, err
	}
	return append([]byte(bucketOperationLogKeyPrefix), buf...), nil
}

func encodeUserOperationLogKey(id influxdb.ID) ([]byte, error) {
	buf, err := id.Encode()
	if err != nil {
		return nil, err
	}
	return append([]byte(userOperationLogKeyPrefix), buf...), nil
}
