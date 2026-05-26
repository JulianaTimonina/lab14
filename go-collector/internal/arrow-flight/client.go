package arrowflight

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/flight"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"energy-monitoring-system/go-collector/pkg/models"
)

// ArrowFlightClient представляет клиент для отправки данных через Arrow Flight RPC
type ArrowFlightClient struct {
	client     flight.Client
	allocator  memory.Allocator
	serverAddr string
	timeout    time.Duration
}

// NewArrowFlightClient создаёт нового клиента Arrow Flight
func NewArrowFlightClient(serverAddr string, timeout time.Duration) (*ArrowFlightClient, error) {
	allocator := memory.NewGoAllocator()
	
	// Создаём gRPC соединение
	conn, err := grpc.NewClient(serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(timeout),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection: %w", err)
	}

	// Создаём Flight клиент
	flightClient, err := flight.NewClientWithMiddleware(conn, nil, nil, nil)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create Flight client: %w", err)
	}

	return &ArrowFlightClient{
		client:     flightClient,
		allocator:  allocator,
		serverAddr: serverAddr,
		timeout:    timeout,
	}, nil
}

// SendAggregatedData отправляет агрегированные данные на сервер Arrow Flight
func (c *ArrowFlightClient) SendAggregatedData(data models.AggregatedData) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Преобразуем данные в Arrow Record
	server := &ArrowFlightServer{allocator: c.allocator}
	record, err := server.ConvertAggregatedDataToRecord(data)
	if err != nil {
		return fmt.Errorf("failed to convert aggregated data to Arrow record: %w", err)
	}
	defer record.Release()

	// Создаём FlightDescriptor для отправки данных
	descriptor := &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte("aggregated-data"),
	}

	// Получаем FlightInfo для определения endpoint
	flightInfo, err := c.client.GetFlightInfo(ctx, descriptor)
	if err != nil {
		return fmt.Errorf("failed to get flight info: %w", err)
	}

	if len(flightInfo.Endpoint) == 0 {
		return fmt.Errorf("no endpoints available for flight")
	}

	// Используем первый endpoint
	endpoint := flightInfo.Endpoint[0]
	
	// Открываем поток для записи данных
	writer, err := c.client.DoPut(ctx)
	if err != nil {
		return fmt.Errorf("failed to open DoPut stream: %w", err)
	}
	defer writer.CloseSend()

	// Отправляем схему
	schema := record.Schema()
	if err := writer.Send(&flight.FlightData{
		FlightDescriptor: descriptor,
		DataHeader:       ipc.MessageSchema(schema, c.allocator),
	}); err != nil {
		return fmt.Errorf("failed to send schema: %w", err)
	}

	// Отправляем данные записи
	recordData, err := ipc.SerializeRecord(record, c.allocator)
	if err != nil {
		return fmt.Errorf("failed to serialize record: %w", err)
	}

	if err := writer.Send(&flight.FlightData{
		FlightDescriptor: descriptor,
		DataBody:         recordData,
	}); err != nil {
		return fmt.Errorf("failed to send record data: %w", err)
	}

	// Получаем ответ от сервера
	_, err = writer.Recv()
	if err != nil {
		return fmt.Errorf("server error during DoPut: %w", err)
	}

	log.Printf("Successfully sent aggregated data via Arrow Flight to %s", c.serverAddr)
	return nil
}

// SendRawData отправляет сырые данные показаний счётчиков
func (c *ArrowFlightClient) SendRawData(readings []models.MeterReading) error {
	// TODO: реализовать преобразование сырых данных в Arrow Record и отправку
	// Это может быть полезно для прямой передачи сырых данных без агрегации
	return fmt.Errorf("SendRawData not implemented yet")
}

// HealthCheck проверяет доступность сервера Arrow Flight
func (c *ArrowFlightClient) HealthCheck() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Простой запрос списка доступных flights
	_, err := c.client.ListFlights(ctx, &flight.Criteria{})
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	return nil
}

// Close закрывает соединение с сервером
func (c *ArrowFlightClient) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

// SendAggregatedDataToServer - удобная обёртка для отправки данных по указанному адресу
func SendAggregatedDataToServer(data models.AggregatedData, serverAddr string) error {
	client, err := NewArrowFlightClient(serverAddr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("failed to create Arrow Flight client: %w", err)
	}
	defer client.Close()

	return client.SendAggregatedData(data)
}