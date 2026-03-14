package app

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jitsucom/bulker/bulkerapp/metrics"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/jitsubase/safego"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/kafkabase"
)

// retryTimeHeader - time of scheduled retry
const retryTimeHeader = "retry_time"
const retriesCountHeader = "retries"
const streamOptionsKeyHeader = "stream_options"
const originalTopicHeader = "original_topic"
const errorHeader = "error"

const pauseHeartBeatInterval = 120 * time.Second

type BatchSizesFunction func(*bulker.StreamOptions) (batchSize int, batchSizeBytes int, retryBatchSize int)
type BatchFunction func(destination *Destination, batchNum, batchSize, batchSizeBytes, retryBatchSize int, highOffset int64, updatedHighOffset int) (counters BatchCounters, state bulker.State, nextBatch bool, err error)
type ShouldConsumeFunction func(partitionId int32, committedOffset, highOffset int64) bool

type BatchConsumer interface {
	Consumer
	RunJob()
	ConsumeAll() (consumed BatchCounters, err error)
	BatchPeriodSec() int
	UpdateBatchPeriod(batchPeriodSec int)
	Options() *bulker.StreamOptions
}

type AbstractBatchConsumer struct {
	sync.Mutex
	*AbstractConsumer
	repository      *Repository
	destinationId   string
	batchPeriodSec  int
	kafkaConfig     *kafka.ConfigMap
	consumerConfig  kafka.ConfigMap
	consumer        atomic.Pointer[kafka.Consumer]
	producerConfig  kafka.ConfigMap
	mode            string
	tableName       string
	waitForMessages time.Duration

	closed chan struct{}

	running           atomic.Bool
	extraRunScheduled atomic.Bool

	//AbstractBatchConsumer marked as no longer needed. We cannot close it immediately because it can be in the middle of processing batch
	retired atomic.Bool
	//idle AbstractBatchConsumer that is not running any batch jobs. retired idle consumer automatically closes itself
	idle atomic.Bool
	//consumer can be paused between batches(idle) and also can be paused during loading batch to destination(not idle)
	paused        atomic.Bool
	resumeChannel chan struct{}

	batchSizeFunc     BatchSizesFunction
	batchFunc         BatchFunction
	shouldConsumeFunc ShouldConsumeFunction
}

func NewAbstractBatchConsumer(repository *Repository, destinationId string, batchPeriodSec int, topicId, mode string, config *Config, kafkaConfig *kafka.ConfigMap, bulkerProducer *Producer, topicManager *TopicManager) (*AbstractBatchConsumer, error) {
	abstract := NewAbstractConsumer(config, repository, topicId, bulkerProducer, topicManager)
	var tableName string
	var err error
	if destinationId != "" {
		_, _, tableName, err = ParseTopicId(topicId)
		if err != nil {
			metrics.ConsumerErrors(topicId, mode, "INVALID_TOPIC", "INVALID_TOPIC", "failed to parse topic").Inc()
			return nil, abstract.NewError("Failed to parse topic: %v", err)
		}
	}

	consumerConfig := kafka.ConfigMap(utils.MapPutAll(kafka.ConfigMap{
		"group.id":                      topicId,
		"auto.offset.reset":             "earliest",
		"allow.auto.create.topics":      false,
		"group.instance.id":             abstract.GetInstanceId(),
		"enable.auto.commit":            false,
		"partition.assignment.strategy": config.KafkaConsumerPartitionsAssigmentStrategy,
		"isolation.level":               "read_committed",
		"session.timeout.ms":            config.KafkaSessionTimeoutMs,
		"fetch.message.max.bytes":       config.KafkaFetchMessageMaxBytes,
		"max.poll.interval.ms":          config.KafkaMaxPollIntervalMs,
	}, *kafkaConfig))

	producerConfig := kafka.ConfigMap(utils.MapPutAll(kafka.ConfigMap{
		"transactional.id":             fmt.Sprintf("%s_failed_%s", topicId, config.InstanceId),
		"queue.buffering.max.messages": config.ProducerQueueSize,
		"batch.size":                   config.ProducerBatchSize,
		"linger.ms":                    config.ProducerLingerMs,
		"compression.type":             config.KafkaTopicCompression,
	}, *kafkaConfig))

	bc := &AbstractBatchConsumer{
		AbstractConsumer: abstract,
		repository:       repository,
		destinationId:    destinationId,
		tableName:        tableName,
		batchPeriodSec:   batchPeriodSec,
		mode:             mode,
		kafkaConfig:      kafkaConfig,
		consumerConfig:   consumerConfig,
		producerConfig:   producerConfig,
		waitForMessages:  time.Duration(config.BatchRunnerWaitForMessagesSec) * time.Second,
		closed:           make(chan struct{}),
		resumeChannel:    make(chan struct{}),
	}
	bc.idle.Store(true)
	return bc, nil
}

func (bc *AbstractBatchConsumer) initTransactionalProducer() (*kafka.Producer, error) {
	//start := time.Now()
	producer, err := kafka.NewProducer(&bc.producerConfig)
	if err != nil {
		metrics.ConsumerErrors(bc.topicId, bc.mode, bc.destinationId, bc.tableName, metrics.KafkaErrorCode(err)).Inc()
		return nil, fmt.Errorf("error creating kafka producer: %v", err)
	}
	err = producer.InitTransactions(nil)
	if err != nil {
		metrics.ConsumerErrors(bc.topicId, bc.mode, bc.destinationId, bc.tableName, metrics.KafkaErrorCode(err)).Inc()
		return nil, fmt.Errorf("error initializing kafka producer transactions: %v", err)
	}
	// Delivery reports channel for 'failed' producer messages
	safego.RunWithRestart(func() {
		ticker := time.NewTicker(time.Second)
		errors := map[string]*int{}
		defer ticker.Stop()
		for {
			select {
			case <-bc.closed:
				bc.Infof("Closing producer.")
				producer.Close()
				return
			case <-ticker.C:
				if len(errors) > 0 {
					for k, v := range errors {
						bc.Errorf("%s COUNT: %d", k, *v)
					}
					clear(errors)
				}
			case e := <-producer.Events():
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						kafkabase.ProducerMessages(ProducerMessageLabels(*ev.TopicPartition.Topic, "error", metrics.KafkaErrorCode(ev.TopicPartition.Error))).Inc()
						errtext := fmt.Sprintf("Error sending message to kafka topic %s: %s", *ev.TopicPartition.Topic, ev.TopicPartition.Error.Error())
						zero := 0
						cnt := utils.MapGetOrCreate(errors, errtext, &zero)
						*cnt++
						//bc.Errorf("%s %d", errtext, len(errors))
					} else {
						kafkabase.ProducerMessages(ProducerMessageLabels(*ev.TopicPartition.Topic, "delivered", "")).Inc()
						//bc.Debugf("Message ID: %s delivered to topic %s [%d] at offset %v", messageId, *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
					}
				case *kafka.Error, kafka.Error:
					bc.Errorf("Producer error: %v", ev)
				case nil:
					bc.Debugf("Producer closed")
					return
				}
			}
		}
	})
	//bc.Infof("Producer initialized in %s", time.Since(start))
	return producer, nil
}

func (bc *AbstractBatchConsumer) BatchPeriodSec() int {
	return bc.batchPeriodSec
}

func (bc *AbstractBatchConsumer) UpdateBatchPeriod(batchPeriodSec int) {
	bc.batchPeriodSec = batchPeriodSec
}

func (bc *AbstractBatchConsumer) TopicId() string {
	return bc.topicId
}

func (bc *AbstractBatchConsumer) RunJob() {
	if bc.running.CompareAndSwap(false, true) {
		startedAt := time.Now()
		defer func() {
			bc.idle.Store(true)
			bc.pauseOrSuspend(startedAt)
			bc.running.Store(false)
		}()
		_, _ = bc.ConsumeAll()
		for bc.extraRunScheduled.CompareAndSwap(true, false) {
			_, _ = bc.ConsumeAll()
		}
	} else {
		bc.extraRunScheduled.Store(true)
	}
}

func (bc *AbstractBatchConsumer) ConsumeAll() (counters BatchCounters, err error) {
	bc.Lock()
	defer bc.Unlock()
	if bc.retired.Load() {
		bc.Errorf("No messages were consumed. Consumer is retired.")
		return BatchCounters{}, bc.NewError("Consumer is retired")
	}
	startedAt := time.Now()
	var totalState bulker.State
	counters.firstOffset = int64(kafka.OffsetBeginning)
	bc.Debugf("Starting consuming messages from topic")
	bc.idle.Store(false)
	commitedOffset := int64(kafka.OffsetBeginning)
	var highOffset int64
	var updatedHighOffset int64
	defer func() {
		sec := time.Since(startedAt).Seconds()
		if err != nil {
			metrics.ConsumerRuns(bc.topicId, bc.mode, bc.destinationId, bc.tableName, "fail").Inc()
			bc.Errorf("Consume finished with error: %v stats: %s offsets: %d-%d time: %.2f s.", err, counters.String(), commitedOffset, highOffset, sec)
		} else {
			metrics.ConsumerRuns(bc.topicId, bc.mode, bc.destinationId, bc.tableName, "success").Inc()
			if counters.processed > 0 {
				bc.Infof("Successfully %s offsets: %d-%d time: %.2f s. AvgSpd: %.2f e/s. States: %s", counters.String(), commitedOffset, highOffset, sec, float64(counters.processed)/sec, totalState.PrintWarehouseState())
			} else {
				countersString := counters.String()
				if countersString != "" {
					if bc.mode == "retry" {
						bc.Infof("Retry consumer finished: %s offsets: %d-%d time: %.2f s.", countersString, commitedOffset, highOffset, sec)
					} else {
						bc.Infof("No messages were processed: %s offsets: %d-%d time: %.2f s.", countersString, commitedOffset, highOffset, sec)
					}
				} else {
					bc.Debugf("No messages were processed. offsets: %d-%d time: %.2f s.", commitedOffset, highOffset, sec)
				}
			}
		}
	}()
	streamOptions := &bulker.StreamOptions{}
	var destination *Destination
	if bc.destinationId != "" {
		destination = bc.repository.LeaseDestination(bc.destinationId)
		if destination == nil {
			bc.Retire()
			return BatchCounters{}, bc.NewError("destination not found: %s. Retiring consumer", bc.destinationId)
		}
		streamOptions = destination.streamOptions
		defer func() {
			destination.Release()
		}()
	}

	maxBatchSize, maxBatchSizeBytes, retryBatchSize := bc.batchSizeFunc(streamOptions)
	consumer, err := bc.initConsumer(false)
	if err != nil {
		bc.errorMetric("resume_error")
		return BatchCounters{}, bc.NewError("Failed to resume kafka consumer: %v", err)
	}

	// Stop heartbeat goroutine from previous run without resuming Kafka partitions.
	// This clears bc.paused so that resume() in processBatch() becomes a no-op —
	// the partition loop handles Pause/Resume at the Kafka level instead.
	bc._unpause()
	for i := 0; i < 100 && bc.paused.Load(); i++ {
		time.Sleep(10 * time.Millisecond)
	}

	// --- PARTITION ASSIGNMENT (replaces hardcoded partition=0) ---
	var assignedPartitions []kafka.TopicPartition

	// Wait for consumer group assignment via Subscribe (both batch and retry consumers)
	for i := 0; i < 30; i++ {
		ass, _ := consumer.Assignment()
		if len(ass) > 0 {
			assignedPartitions = ass
			break
		}
		consumer.Poll(1000) // triggers group join + rebalance
	}
	if len(assignedPartitions) == 0 {
		bc.Debugf("No partitions assigned — standby mode")
		return BatchCounters{}, nil
	}
	bc.Infof("Assigned %d partition(s): %v", len(assignedPartitions), partitionIds(assignedPartitions))

	// --- PER-PARTITION PROCESSING ---
	for _, tp := range assignedPartitions {
		if bc.retired.Load() {
			return
		}
		if bc.destinationId != "" {
			currentDst := bc.repository.GetDestination(bc.destinationId)
			if currentDst == nil || currentDst.configHash != destination.configHash {
				bc.Infof("Destination config changed. Stopping.")
				return
			}
		}

		partition := tp.Partition

		// Check if assignment changed due to rebalance (compare actual partitions, not just count)
		currentAss, _ := consumer.Assignment()
		if !samePartitions(assignedPartitions, currentAss) {
			bc.Infof("Assignment changed during processing (was %v, now %v). Aborting to re-init.", partitionIds(assignedPartitions), partitionIds(currentAss))
			return
		}

		// Pause all, resume only current partition
		if len(assignedPartitions) > 1 {
			if pauseErr := consumer.Pause(assignedPartitions); pauseErr != nil {
				bc.Errorf("Failed to pause partitions: %v", pauseErr)
				return
			}
			if resumeErr := consumer.Resume([]kafka.TopicPartition{tp}); resumeErr != nil {
				bc.Errorf("Failed to resume partition %d: %v", partition, resumeErr)
				return
			}
		}

		// Per-partition watermark + committed offset
		_, highOffset, err = consumer.QueryWatermarkOffsets(bc.topicId, partition, 10_000)
		updatedHighOffset = highOffset
		if err != nil {
			bc.errorMetric("query_watermark_failed")
			bc.Errorf("Failed to query watermark for partition %d: %v", partition, err)
			continue
		}

		commitedOffset = int64(kafka.OffsetBeginning)
		offsets, erro := consumer.Committed([]kafka.TopicPartition{{Topic: &bc.topicId, Partition: partition}}, 10_000)
		if len(offsets) > 0 {
			if offsets[0].Offset != kafka.OffsetInvalid {
				commitedOffset = int64(offsets[0].Offset)
			} else {
				bc.Infof("No committed offset for partition %d (new partition). Starting from beginning.", partition)
			}
		} else {
			bc.Errorf("Failed to query commited offsets for partition %d: %v", partition, erro)
		}

		if !bc.shouldConsume(partition, commitedOffset, highOffset) {
			bc.Debugf("Partition %d: nothing to consume (%d-%d)", partition, commitedOffset, highOffset)
			continue
		}

		lastOffsetQueryTime := time.Now()
		partitionStart := time.Now()
		// Time budget per partition: batch period / number of assigned partitions.
		// Ensures all partitions get serviced within one ConsumeAll cycle.
		maxPartitionTime := time.Duration(bc.batchPeriodSec) * time.Second / time.Duration(len(assignedPartitions))
		if maxPartitionTime < 30*time.Second {
			maxPartitionTime = 30 * time.Second
		}
		bc.Debugf("Starting consuming partition %d. Messages: ~%d. Time budget: %s", partition, highOffset-commitedOffset, maxPartitionTime)
		batchNumber := 1
		for {
			if bc.retired.Load() {
				return
			}
			if bc.destinationId != "" {
				currentDst := bc.repository.GetDestination(bc.destinationId)
				if currentDst == nil || currentDst.configHash != destination.configHash {
					bc.Infof("Destination config has changed. Finishing this batch.")
					return
				}
			}
			// Check for rebalance between batches (not just between partitions)
			currentAss, _ := consumer.Assignment()
			if !samePartitions(assignedPartitions, currentAss) {
				bc.Infof("Assignment changed during batch processing (was %v, now %v). Aborting to re-init.", partitionIds(assignedPartitions), partitionIds(currentAss))
				return
			}
			batchCounters, batchState, nextBatch, err2 := bc.processBatch(destination, batchNumber, maxBatchSize, maxBatchSizeBytes, retryBatchSize, highOffset, int(updatedHighOffset))
			if err2 != nil {
				if nextBatch {
					bc.Errorf("Batch finished with error: %v stats: %s nextBatch: %t", err2, batchCounters, nextBatch)
				}
			}
			bc.countersMetric(batchCounters)
			totalState.Merge(batchState)
			counters.accumulate(batchCounters)
			if batchCounters.consumed > 0 {
				if time.Since(lastOffsetQueryTime) > 1*time.Minute || !nextBatch {
					var err1 error
					consumer = bc.consumer.Load()
					_, updatedHighOffset, err1 = consumer.QueryWatermarkOffsets(bc.topicId, partition, 10_000)
					if err1 != nil {
						bc.Errorf("Failed to query watermark offsets: %v", err1)
						bc.errorMetric("query_watermark_failed")
					}
					lastOffsetQueryTime = time.Now()
				}
				queueSize := math.Max(float64(updatedHighOffset-batchCounters.firstOffset-int64(batchCounters.consumed)), 0)
				metrics.ConsumerQueueSize(bc.topicId, bc.mode, bc.destinationId, bc.tableName).Set(queueSize)
			}
			if !nextBatch {
				if err2 != nil {
					err = err2
					return // terminal error — stop processing all partitions
				}
				break // reached watermark — move to next partition
			}
			// Time budget exceeded — yield to next partition
			if len(assignedPartitions) > 1 && time.Since(partitionStart) >= maxPartitionTime {
				bc.Infof("Partition %d time budget exceeded (%s). Yielding to next partition.", partition, time.Since(partitionStart).Round(time.Second))
				break
			}
			batchNumber++
		}
	}
	return
}

func (bc *AbstractBatchConsumer) close() error {
	select {
	case <-bc.closed:
	default:
		close(bc.closed)
	}
	consumer := bc.consumer.Swap(nil)
	if consumer != nil {
		err := consumer.Close()
		return err
	}
	return nil
}

func (bc *AbstractBatchConsumer) processBatch(destination *Destination, batchNum, batchSize, batchSizeBytes, retryBatchSize int, highOffset int64, updatedHighOffset int) (counters BatchCounters, state bulker.State, nextBath bool, err error) {
	bc.resume()
	return bc.batchFunc(destination, batchNum, batchSize, batchSizeBytes, retryBatchSize, highOffset, updatedHighOffset)
}

func (bc *AbstractBatchConsumer) shouldConsume(partitionId int32, committedOffset, highOffset int64) bool {
	if highOffset == 0 || committedOffset == highOffset {
		return false
	}
	if bc.shouldConsumeFunc != nil {
		bc.resume()
		return bc.shouldConsumeFunc(partitionId, committedOffset, highOffset)
	}
	return true
}

func (bc *AbstractBatchConsumer) pauseOrSuspend(startedAt time.Time) {
	if bc.idle.Load() && bc.retired.Load() {
		// Close retired idling consumer
		bc.Infof("Consumer is retired. Closing")
		_ = bc.close()
		return
	}
	consumer := bc.consumer.Load()
	if consumer == nil {
		return
	}
	batchPeriodSec := bc.BatchPeriodSec()
	timeToNextBatch := time.Duration(batchPeriodSec)*time.Second - time.Since(startedAt)
	if bc.config.SuspendConsumers && timeToNextBatch >= 60*time.Second {
		bc.Infof("Suspending consumer %s for %s", consumer.String(), timeToNextBatch)
		bc._unpause()
		_ = consumer.Close()
		bc.consumer.Store(nil)
	} else {
		bc.pause(false)
	}
}

// pause consumer.
func (bc *AbstractBatchConsumer) pause(immediatePoll bool) {
	if !bc.paused.CompareAndSwap(false, true) {
		return
	}
	bc.pauseKafkaConsumer()

	safego.RunWithRestart(func() {
		errorReported := false
		firstPoll := immediatePoll
		//this loop keeps heatbeating consumer to prevent it from being kicked out from group
		pauseTicker := time.NewTicker(time.Duration(bc.config.KafkaMaxPollIntervalMs) * time.Millisecond / 2)
		defer pauseTicker.Stop()
	loop:
		for {
			if bc.idle.Load() && bc.retired.Load() {
				// Close retired idling consumer
				bc.Infof("Consumer is retired. Closing")
				_ = bc.close()
				return
			}
			if !firstPoll {
				select {
				case <-bc.resumeChannel:
					bc.paused.CompareAndSwap(true, false)
					bc.Debugf("Consumer resumed.")
					break loop
				case <-pauseTicker.C:
				}
			}
			firstPoll = false
			consumer := bc.consumer.Load()
			if consumer == nil {
				bc.Errorf("Paused Consumer is nil.")
				continue
			}
			message, err := consumer.ReadMessage(bc.waitForMessages)
			if err != nil {
				kafkaErr := err.(kafka.Error)
				if kafkaErr.Code() == kafka.ErrTimedOut {
					bc.Debugf("Consumer paused. Heartbeat sent.")
					continue
				}
				bc.errorMetric("error_while_paused")
				if !errorReported {
					bc.Errorf("Error on paused consumer: %v", kafkaErr)
					errorReported = true
				}
				if kafkaErr.IsRetriable() {
					time.Sleep(10 * time.Second)
				} else {
					bc.restartConsumer(nil)
				}
			} else if message != nil {
				bc.Debugf("Unexpected message on paused consumer: %v", message)
				//If message slipped through pause, rollback offset and make sure consumer is paused
				_, err = consumer.SeekPartitions([]kafka.TopicPartition{message.TopicPartition})
				if err != nil {
					bc.errorMetric("ROLLBACK_ON_PAUSE_ERR")
					bc.SystemErrorf("Failed to rollback offset on paused consumer: %v", err)
					bc.restartConsumer(nil)
				}
				bc.pauseKafkaConsumer()
			}
		}
	})
}

func (bc *AbstractBatchConsumer) initConsumer(force bool) (consumer *kafka.Consumer, err error) {
	consumer = bc.consumer.Load()
	if consumer == nil || force {
		consumer, err = kafka.NewConsumer(&bc.consumerConfig)
		if err != nil {
			bc.errorMetric("consumer_error:" + metrics.KafkaErrorCode(err))
			bc.Errorf("Error creating kafka consumer: %v", err)
			return nil, err
		}
		err = consumer.SubscribeTopics([]string{bc.topicId}, bc.rebalanceCallback)
		if err != nil {
			bc.errorMetric("consumer_error:" + metrics.KafkaErrorCode(err))
			_ = consumer.Close()
			bc.Errorf("Failed to subscribe to topic: %v", err)
			return nil, err
		}
		// Removed manual Assign() for retry topic — let Kafka consumer group
		// handle partition distribution automatically (same as batch consumer).
		// The previous code forced partition=InstanceIndex which conflicted with
		// the group-assigned partition, leaving some partitions unread.
		bc.Infof("Consumer created: %s", consumer.String())
		bc.consumer.Store(consumer)
	}
	return consumer, nil
}

func (bc *AbstractBatchConsumer) restartConsumer(beforeInit func()) {
	if bc.retired.Load() {
		return
	}
	bc.Infof("Restarting consumer")
	go func(c *kafka.Consumer) {
		e1 := c.Unsubscribe()
		e2 := c.Close()
		bc.Infof("Previous consumer closed: %v %v", e1, e2)
	}(bc.consumer.Load())

	ticker := time.NewTicker(time.Duration(bc.config.KafkaSessionTimeoutMs+15000) * time.Millisecond)
	defer ticker.Stop()
	// for faster reaction on retiring
	pauseTicker := time.NewTicker(1 * time.Second)
	defer pauseTicker.Stop()

	for {
		select {
		case <-pauseTicker.C:
			if bc.idle.Load() && bc.retired.Load() {
				return
			}
		case <-ticker.C:
			if beforeInit != nil {
				beforeInit()
			}
			_, err := bc.initConsumer(true)
			if err != nil {
				break
			}
			return
		}
	}
}

func (bc *AbstractBatchConsumer) pauseKafkaConsumer() {
	consumer := bc.consumer.Load()
	partitions, err := consumer.Assignment()
	if len(partitions) > 0 {
		err = consumer.Pause(partitions)
	}
	if err != nil {
		bc.errorMetric("pause_error")
		bc.SystemErrorf("Failed to pause kafka consumer: %v", err)
		bc.restartConsumer(nil)
	} else {
		if len(partitions) > 0 {
			bc.Debugf("Consumer paused.")
		}
		// otherwise rebalanceCallback will handle pausing
	}
}

func (bc *AbstractBatchConsumer) rebalanceCallback(consumer *kafka.Consumer, event kafka.Event) error {
	assignedParts, ok := event.(kafka.AssignedPartitions)
	bc.Debugf("Rebalance event: %v . Paused: %t", event, bc.paused.Load())
	if ok && bc.paused.Load() {
		err := consumer.Pause(assignedParts.Partitions)
		if err != nil {
			bc.errorMetric("pause_error")
			bc.SystemErrorf("Failed to pause kafka consumer: %v", err)
			return err
		} else {
			bc.Debugf("Consumer paused.")
		}
	}
	return nil
}

func (bc *AbstractBatchConsumer) _unpause() {
	if !bc.paused.Load() {
		return
	}
	select {
	case bc.resumeChannel <- struct{}{}:
		return
	case <-time.After(time.Duration(bc.config.KafkaMaxPollIntervalMs) * time.Millisecond):
		bc.errorMetric("resume_error")
		bc.SystemErrorf("failed to unpause kafka consumer.")
	}
}

func (bc *AbstractBatchConsumer) resume() {
	if !bc.paused.Load() {
		return
	}
	consumer := bc.consumer.Load()
	var err error
	defer func() {
		if err != nil {
			bc.errorMetric("resume_error")
			bc.SystemErrorf("failed to resume kafka consumer.: %v", err)
		}
	}()
	partitions, err := consumer.Assignment()
	if err != nil {
		return
	}
	select {
	case bc.resumeChannel <- struct{}{}:
		err = consumer.Resume(partitions)
	case <-time.After(time.Duration(bc.config.KafkaMaxPollIntervalMs) * time.Millisecond):
		err = bc.NewError("Resume timeout.")
		//return bc.consumer.Resume(partitions)
	}
}

// Retire Mark consumer as retired
// Consumer will close itself when com
func (bc *AbstractBatchConsumer) Retire() {
	bc.Infof("Retiring %s consumer", bc.mode)
	bc.retired.Store(true)
}
func (bc *AbstractBatchConsumer) errorMetric(errorType string) {
	metrics.ConsumerErrors(bc.topicId, bc.mode, bc.destinationId, bc.tableName, errorType).Inc()
}

func (bc *AbstractBatchConsumer) Options() *bulker.StreamOptions {
	if bc.destinationId != "" {
		currentDst := bc.repository.GetDestination(bc.destinationId)
		if currentDst != nil {
			return currentDst.streamOptions
		}
	}
	return &bulker.StreamOptions{}
}

func (bc *AbstractBatchConsumer) countersMetric(counters BatchCounters) {
	countersValue := reflect.ValueOf(counters)
	countersType := countersValue.Type()
	for i := 0; i < countersValue.NumField(); i++ {
		metricName := countersType.Field(i).Name
		if metricName == "firstOffset" {
			continue
		}
		value := countersValue.Field(i).Int()
		if value > 0 {
			metrics.ConsumerMessages(bc.topicId, bc.mode, bc.destinationId, bc.tableName, metricName).Add(float64(value))
			if metricName == "processed" {
				metrics.ConnectionMessageStatuses(bc.destinationId, bc.tableName, "success").Add(float64(value))
			} else if metricName == "failed" {
				metrics.ConnectionMessageStatuses(bc.destinationId, bc.tableName, "error").Add(float64(value))
			} else if metricName == "retried" || metricName == "deadLettered" {
				metrics.ConnectionMessageStatuses(bc.destinationId, bc.tableName, metricName).Add(float64(value))
			}
		}
	}
}

func partitionIds(tps []kafka.TopicPartition) []int32 {
	ids := make([]int32, len(tps))
	for i, tp := range tps {
		ids[i] = tp.Partition
	}
	return ids
}

func samePartitions(a, b []kafka.TopicPartition) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[int32]struct{}, len(a))
	for _, tp := range a {
		set[tp.Partition] = struct{}{}
	}
	for _, tp := range b {
		if _, ok := set[tp.Partition]; !ok {
			return false
		}
	}
	return true
}

type BatchCounters struct {
	consumed        int
	skipped         int
	processed       int
	processedBytes  int
	notReadyReadded int
	retryScheduled  int
	retried         int
	deadLettered    int
	failed          int
	firstOffset     int64
}

// accumulate stats from batch
func (bs *BatchCounters) accumulate(batchStats BatchCounters) {
	bs.consumed += batchStats.consumed
	bs.processedBytes += batchStats.processedBytes
	bs.skipped += batchStats.skipped
	bs.processed += batchStats.processed
	bs.notReadyReadded += batchStats.notReadyReadded
	bs.retryScheduled += batchStats.retryScheduled
	bs.deadLettered += batchStats.deadLettered
	bs.retried += batchStats.retried
	if bs.firstOffset < 0 && batchStats.firstOffset >= 0 {
		bs.firstOffset = batchStats.firstOffset
	}
}

// to string
func (bs *BatchCounters) String() string {
	// print non-zero values
	var sb strings.Builder
	countersValue := reflect.ValueOf(*bs)
	countersType := countersValue.Type()
	for i := 0; i < countersValue.NumField(); i++ {
		value := countersValue.Field(i).Int()
		if value > 0 {
			sb.WriteString(fmt.Sprintf("%s: %d ", countersType.Field(i).Name, value))
		}
	}
	return sb.String()
}
