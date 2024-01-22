package spqrparser

var reservedWords = map[string]int{
	"show":       SHOW,
	"kill":       KILL,
	"column":     COLUMN,
	"columns":    COLUMNS,
	"shard":      SHARD,
	"rule":       RULE,
	"sharding":   SHARDING,
	"create":     CREATE,
	"add":        ADD,
	"key":        KEY,
	"for":        FOR,
	"range":      RANGE,
	"lock":       LOCK,
	"unlock":     UNLOCK,
	"drop":       DROP,
	"all":        ALL,
	"shutdown":   SHUTDOWN,
	"split":      SPLIT,
	"from":       FROM,
	"by":         BY,
	"to":         TO,
	"with":       WITH,
	"unite":      UNITE,
	"listen":     LISTEN,
	"register":   REGISTER,
	"unregister": UNREGISTER,
	"router":     ROUTER,
	"move":       MOVE,
	"address":    ADDRESS,
	"host":       HOST,
	"route":      ROUTE,
	"keysapce":   KEYSPACE,
	"table":      TABLE,
	"hash":       HASH,
	"function":   FUNCTION,
	"where":      WHERE,
	"set":        SET,
	"or":         OR,
	"and":        AND,
	"start":      START,
	"stop":       STOP,
	"trace":      TRACE,
	"messages":   MESSAGES,
	"client":     CLIENT,
	"cascade":    CASCADE,
	"murmur":     MURMUR,
	"city":       CITY,
	"identity":   IDENTITY,
	"ident":      IDENTITY,
	"attach":     ATTACH,
	"types":      TYPES,
	"varchar":    VARCHAR,
	"int":        INT,
	"integer":    INTEGER,
	"alter":      ALTER,
	"relations":  RELATIONS,
	"in":         IN,
}
