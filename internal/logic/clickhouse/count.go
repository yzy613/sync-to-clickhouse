package clickhouse

func (s *sClickHouse) SetCountFlush(count uint) {
	s.flushCount = count
}
