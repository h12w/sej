package hub

import (
	"encoding/json"
)

func js(v interface{}) string {
	buf, _ := json.MarshalIndent(v, "", "    ")
	return string(buf)
}
