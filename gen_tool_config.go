package gen_tool_core


type GenToolConfig struct {
	Amqp         string `json:"amqp"`
	ExchangeType string `json:"exchangeType"`
	Exchange     string `json:"exchange"`
	WorkMode     string `json:"workMode"`

	Priority 	 uint8 `json:"priority"`
}