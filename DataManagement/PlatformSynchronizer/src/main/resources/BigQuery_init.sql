


CREATE VIEW desisensors.Records.HeatingLearningData AS SELECT data.DateTime, data.Date, data.Value, names.SensorName, names.SensorId, types.Type, data.AggregationScope FROM desisensors.Records.SensorDataAggregated data, desisensors.Records.SensorNames names, desisensors.Records.SensorTypes types WHERE data.AggregationScope=4 AND names.Monitor=true AND names.SensorId=data.SensorId AND names.Type=types.Id ORDER BY data.DateTime DESC