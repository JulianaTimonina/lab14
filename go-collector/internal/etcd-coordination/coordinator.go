package etcdcoordination

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	collectorsPrefix = "/collectors/"
	shardsPrefix     = "/shards/"
)

// Coordinator управляет координацией сборщиков через etcd
type Coordinator struct {
	client      *clientv3.Client
	collectorID string
	shards      []string
	allShards   []string
	leaseID     clientv3.LeaseID
}

// NewCoordinator создаёт новый координатор
func NewCoordinator(endpoints []string, collectorID string, allShards []string) (*Coordinator, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}

	return &Coordinator{
		client:      cli,
		collectorID: collectorID,
		shards:      make([]string, 0),
		allShards:   allShards,
	}, nil
}

// Register регистрирует сборщик в etcd
func (c *Coordinator) Register(ctx context.Context) error {
	// Создаём lease для автоматического удаления при отключении
	resp, err := c.client.Grant(ctx, 10)
	if err != nil {
		return fmt.Errorf("failed to create lease: %w", err)
	}
	c.leaseID = resp.ID

	// Регистрируем сборщик
	key := collectorsPrefix + c.collectorID
	value := fmt.Sprintf(`{"id": "%s", "timestamp": %d}`, c.collectorID, time.Now().Unix())
	_, err = c.client.Put(ctx, key, value, clientv3.WithLease(c.leaseID))
	if err != nil {
		return fmt.Errorf("failed to register collector: %w", err)
	}

	// Запускаем keep-alive
	keepAlive, err := c.client.KeepAlive(ctx, c.leaseID)
	if err != nil {
		return fmt.Errorf("failed to start keep-alive: %w", err)
	}

	// Читаем keep-alive responses чтобы канал не блокировался
	go func() {
		for range keepAlive {
			// keep-alive работает
		}
	}()

	log.Printf("Collector %s registered with lease ID %d", c.collectorID, c.leaseID)
	return nil
}

// AssignShards назначает шарды сборщику
func (c *Coordinator) AssignShards(ctx context.Context, shards []string) error {
	c.shards = shards

	// Сохраняем назначение шардов
	key := shardsPrefix + c.collectorID
	shardsJSON, err := json.Marshal(shards)
	if err != nil {
		return fmt.Errorf("failed to marshal shards: %w", err)
	}

	_, err = c.client.Put(ctx, key, string(shardsJSON), clientv3.WithLease(c.leaseID))
	if err != nil {
		return fmt.Errorf("failed to assign shards: %w", err)
	}

	log.Printf("Assigned shards to collector %s: %v", c.collectorID, shards)
	return nil
}

// GetAssignedShards возвращает назначенные шарды
func (c *Coordinator) GetAssignedShards() []string {
	return c.shards
}

// SetAllShards устанавливает список всех шардов для перебалансировки
func (c *Coordinator) SetAllShards(shards []string) {
	c.allShards = shards
}

// WatchShardsChanges отслеживает изменения в назначении шардов
func (c *Coordinator) WatchShardsChanges(ctx context.Context, callback func(shards []string)) error {
	key := shardsPrefix + c.collectorID
	watchChan := c.client.Watch(ctx, key)

	go func() {
		for watchResp := range watchChan {
			for _, event := range watchResp.Events {
				if event.Type == clientv3.EventTypePut {
					var shards []string
					if err := json.Unmarshal(event.Kv.Value, &shards); err == nil {
						c.shards = shards
						callback(shards)
					}
				} else if event.Type == clientv3.EventTypeDelete {
					c.shards = []string{}
					callback([]string{})
				}
			}
		}
	}()

	return nil
}

// DiscoverCollectors возвращает список активных сборщиков
func (c *Coordinator) DiscoverCollectors(ctx context.Context) ([]string, error) {
	resp, err := c.client.Get(ctx, collectorsPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get collectors: %w", err)
	}

	collectors := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		collectorID := strings.TrimPrefix(key, collectorsPrefix)
		collectors = append(collectors, collectorID)
	}

	return collectors, nil
}

// RebalanceShards перераспределяет шарды между сборщиками
// Если allShards пустой, используется внутренний список шардов (c.allShards)
func (c *Coordinator) RebalanceShards(ctx context.Context, allShards []string) error {
	// Если allShards не передан, используем внутренний список
	shardsToBalance := allShards
	if len(shardsToBalance) == 0 {
		if len(c.allShards) == 0 {
			return fmt.Errorf("no shards available for rebalancing")
		}
		shardsToBalance = c.allShards
	}

	collectors, err := c.DiscoverCollectors(ctx)
	if err != nil {
		return fmt.Errorf("failed to discover collectors: %w", err)
	}

	if len(collectors) == 0 {
		return fmt.Errorf("no collectors available")
	}

	// Простое распределение: round-robin
	shardsPerCollector := len(shardsToBalance) / len(collectors)
	remainder := len(shardsToBalance) % len(collectors)

	shardIndex := 0
	for i, collectorID := range collectors {
		start := shardIndex
		end := start + shardsPerCollector
		if i < remainder {
			end++
		}

		if start >= len(shardsToBalance) {
			break
		}

		if end > len(shardsToBalance) {
			end = len(shardsToBalance)
		}

		assignedShards := shardsToBalance[start:end]
		shardIndex = end

		// Сохраняем назначение
		key := shardsPrefix + collectorID
		shardsJSON, err := json.Marshal(assignedShards)
		if err != nil {
			return fmt.Errorf("failed to marshal shards: %w", err)
		}

		_, err = c.client.Put(ctx, key, string(shardsJSON))
		if err != nil {
			return fmt.Errorf("failed to assign shards to collector %s: %w", collectorID, err)
		}
	}

	log.Printf("Rebalanced %d shards among %d collectors", len(shardsToBalance), len(collectors))
	return nil
}

// RebalanceShardsAuto автоматически перераспределяет шарды при изменении количества сборщиков
func (c *Coordinator) RebalanceShardsAuto(ctx context.Context) error {
	return c.RebalanceShards(ctx, nil)
}

// WatchCollectorsChanges отслеживает изменения в регистрации сборщиков и запускает перебалансировку
func (c *Coordinator) WatchCollectorsChanges(ctx context.Context) error {
	watchChan := c.client.Watch(ctx, collectorsPrefix, clientv3.WithPrefix())
	
	go func() {
		for watchResp := range watchChan {
			// Игнорируем события, если нет изменений
			if len(watchResp.Events) == 0 {
				continue
			}
			
			// Логируем изменения
			for _, event := range watchResp.Events {
				collectorID := strings.TrimPrefix(string(event.Kv.Key), collectorsPrefix)
				if event.Type == clientv3.EventTypePut {
					log.Printf("Collector %s registered or updated", collectorID)
				} else if event.Type == clientv3.EventTypeDelete {
					log.Printf("Collector %s removed", collectorID)
				}
			}
			
			// Запускаем перебалансировку с задержкой, чтобы избежать частых перераспределений
			time.Sleep(1 * time.Second)
			
			if err := c.RebalanceShardsAuto(ctx); err != nil {
				log.Printf("Failed to auto-rebalance shards: %v", err)
			} else {
				log.Printf("Auto-rebalanced shards after collectors change")
			}
		}
	}()
	
	return nil
}

// Close закрывает соединение с etcd
func (c *Coordinator) Close() error {
	return c.client.Close()
}