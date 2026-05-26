package arrowflight

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/flight"
	"github.com/apache/arrow/go/v14/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"google.golang.org/grpc"

	"energy-monitoring-system/go-collector/pkg/models"
)

// ArrowFlightServer реализует сервер Flight RPC для передачи агрегированных данных
type ArrowFlightServer struct {
	flight.BaseFlightServer
	allocator memory.Allocator
	port      int
	server    *grpc.Server
}

// NewArrowFlightServer создаёт новый сервер Arrow Flight
func NewArrowFlightServer(port int) *ArrowFlightServer {
	return &ArrowFlightServer{
		allocator: memory.NewGoAllocator(),
		port:      port,
	}
}

// Start запускает сервер
func (s *ArrowFlightServer) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s.server = grpc.NewServer()
	flight.RegisterFlightServiceServer(s.server, s)

	log.Printf("Arrow Flight server starting on port %d", s.port)
	return s.server.Serve(lis)
}

// Stop останавливает сервер
func (s *ArrowFlightServer) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
}

// GetFlightInfo возвращает информацию о доступных данных
func (s *ArrowFlightServer) GetFlightInfo(ctx context.Context, request *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	// Возвращаем информацию о доступных потоках данных
	// В реальной системе здесь может быть несколько endpoints для разных типов данных
	ticket := &flight.Ticket{
		Ticket: []byte("aggregated-data"),
	}

	schema := s.createAggregatedDataSchema()

	return &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket: ticket,
				Location: []*flight.Location{
					{
						Uri: fmt.Sprintf("grpc://localhost:%d", s.port),
					},
				},
			},
		},
		Schema:           flight.SerializeSchema(schema, s.allocator),
		FlightDescriptor: request,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

// DoGet возвращает поток данных
func (s *ArrowFlightServer) DoGet(request *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	// В реальной системе здесь нужно получать данные из канала или буфера
	// Для примера создадим тестовые данные

	schema := s.createAggregatedDataSchema()
	record := s.createTestRecord(schema)

	writer := ipc.NewWriter(stream, ipc.WithSchema(schema))
	defer writer.Close()

	return writer.Write(record)
}

// createAggregatedDataSchema создаёт схему Arrow для агрегированных данных
func (s *ArrowFlightServer) createAggregatedDataSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "window_start", Type: arrow.FixedWidthTypes.Timestamp_ns},
		{Name: "window_end", Type: arrow.FixedWidthTypes.Timestamp_ns},
		{Name: "meter_id", Type: arrow.BinaryTypes.String},
		{Name: "sum", Type: arrow.PrimitiveTypes.Float64},
		{Name: "avg", Type: arrow.PrimitiveTypes.Float64},
		{Name: "min", Type: arrow.PrimitiveTypes.Float64},
		{Name: "max", Type: arrow.PrimitiveTypes.Float64},
		{Name: "count", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
}

// createTestRecord создаёт тестовую запись для демонстрации
func (s *ArrowFlightServer) createTestRecord(schema *arrow.Schema) arrow.Record {
	builder := array.NewRecordBuilder(s.allocator, schema)
	defer builder.Release()

	// Добавляем тестовые данные
	timestampType := arrow.FixedWidthTypes.Timestamp_ns
	now := arrow.Timestamp(time.Now().UnixNano())
	windowEnd := arrow.Timestamp(time.Now().Add(30 * time.Second).UnixNano())

	// window_start
	builder.Field(0).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{now}, nil)
	// window_end
	builder.Field(1).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{windowEnd}, nil)
	// meter_id
	builder.Field(2).(*array.StringBuilder).AppendValues([]string{"meter-001"}, nil)
	// sum
	builder.Field(3).(*array.Float64Builder).AppendValues([]float64{150.5}, nil)
	// avg
	builder.Field(4).(*array.Float64Builder).AppendValues([]float64{5.02}, nil)
	// min
	builder.Field(5).(*array.Float64Builder).AppendValues([]float64{0.5}, nil)
	// max
	builder.Field(6).(*array.Float64Builder).AppendValues([]float64{10.2}, nil)
	// count
	builder.Field(7).(*array.Int64Builder).AppendValues([]int64{30}, nil)

	return builder.NewRecord()
}

// ConvertAggregatedDataToRecord преобразует AggregatedData в Arrow Record
func (s *ArrowFlightServer) ConvertAggregatedDataToRecord(data models.AggregatedData) (arrow.Record, error) {
	schema := s.createAggregatedDataSchema()
	builder := array.NewRecordBuilder(s.allocator, schema)
	defer builder.Release()

	// Подсчитываем общее количество строк (по одному на каждый счётчик в агрегатах)
	numRows := len(data.Aggregates)
	if numRows == 0 {
		return builder.NewRecord(), nil
	}

	// Подготавливаем срезы для каждой колонки
	windowStarts := make([]arrow.Timestamp, numRows)
	windowEnds := make([]arrow.Timestamp, numRows)
	meterIDs := make([]string, numRows)
	sums := make([]float64, numRows)
	avgs := make([]float64, numRows)
	mins := make([]float64, numRows)
	maxs := make([]float64, numRows)
	counts := make([]int64, numRows)

	i := 0
	for meterID, agg := range data.Aggregates {
		windowStarts[i] = arrow.Timestamp(data.WindowStart.UnixNano())
		windowEnds[i] = arrow.Timestamp(data.WindowEnd.UnixNano())
		meterIDs[i] = meterID
		sums[i] = agg.Sum
		avgs[i] = agg.Avg
		mins[i] = agg.Min
		maxs[i] = agg.Max
		counts[i] = int64(agg.Count)
		i++
	}

	// Заполняем builders
	builder.Field(0).(*array.TimestampBuilder).AppendValues(windowStarts, nil)
	builder.Field(1).(*array.TimestampBuilder).AppendValues(windowEnds, nil)
	builder.Field(2).(*array.StringBuilder).AppendValues(meterIDs, nil)
	builder.Field(3).(*array.Float64Builder).AppendValues(sums, nil)
	builder.Field(4).(*array.Float64Builder).AppendValues(avgs, nil)
	builder.Field(5).(*array.Float64Builder).AppendValues(mins, nil)
	builder.Field(6).(*array.Float64Builder).AppendValues(maxs, nil)
	builder.Field(7).(*array.Int64Builder).AppendValues(counts, nil)

	return builder.NewRecord(), nil
}

// SendAggregatedData отправляет агрегированные данные через Flight RPC
// (это клиентская функция, но размещена здесь для удобства)
func SendAggregatedData(data models.AggregatedData, flightServerURL string) error {
	return SendAggregatedDataToServer(data, flightServerURL)
}