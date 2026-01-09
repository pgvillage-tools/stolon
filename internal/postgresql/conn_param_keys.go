package postgresql

type ConnParamKey string

const (
	ConnParamKeyHost ConnParamKey = "host"
	ConnParamKeyPort ConnParamKey = "port"
	ConnParamKeyUser ConnParamKey = "user"
)
