class OutputTopics:
    def __init__(self, output_topic, target_database) -> None:
        self.output_topic: str = output_topic
        self.target_database: str = target_database

class RawSettings:    
    def __init__(self,
            raw_data_name,
            producer_name,
            producer_port,
            db_host,
            db_port,
            consumer_group,
            output_topics=None,
            expected_cost=5.0) -> None:
        self.raw_data_name: str = raw_data_name
        self.producer_name: str = producer_name
        self.producer_port: int = producer_port
        self.db_host: str = db_host
        self.db_port: str = db_port
        self.consumer_group: str = consumer_group
        self.output_topics: list[OutputTopics] | None = output_topics
        self.expected_cost = expected_cost
        
class RawStatistics:
    def __init__(self, write_time_ns, write_size_byte) -> None:
        self.write_time_ns: float = write_time_ns
        self.write_size_byte: float = write_size_byte

class RawStatisticsBook:
    def __init__(self,
                    producer_name,
                    raw_name,
                    record_count,
                    write_time_ns_total,
                    write_size_byte_total
                 ) -> None:
        self.producer_name: str = producer_name
        self.raw_name:str = raw_name
        self.record_count: int = record_count
        self.write_time_ns_total: float = write_time_ns_total
        self.write_size_byte_total: float = write_size_byte_total
        
    def add_raw_statistic(self, rawStat:RawStatistics):
        self.record_count = self.record_count + 1
        self.write_time_ns_total = self.write_time_ns_total + rawStat.write_time_ns
        self.write_size_byte_total = self.write_size_byte_total + rawStat.write_size_byte
    
    def reset_stats(self):
        self.record_count = 0
        self.write_size_byte_total = 0.0
        self.write_time_ns_total = 0.0
   