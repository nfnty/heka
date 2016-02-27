/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2014
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   nfnty
#
# ***** END LICENSE BLOCK *****/

package nfnty

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

// RFC3339Micro is a time layout without timezone
const RFC3339Micro string = "2006-01-02T15:04:05.999999"

var re = regexp.MustCompile(".*\\.(.*)")

// UlogdDecoder is the backbone of the plugin
type UlogdDecoder struct{}

// UlogdDecoderConfig contains user configuration
type UlogdDecoderConfig struct{}

// ConfigStruct initializes the configuration with defaults
func (decoder *UlogdDecoder) ConfigStruct() interface{} {
	return new(UlogdDecoderConfig)
}

// Init initializes the plugin
func (decoder *UlogdDecoder) Init(config interface{}) (err error) {
	return
}

func decodeJSON(key string, value interface{}) (field *message.Field, err error) {
	switch vtype := value.(type) {
	case string:
		if pValue, e := strconv.ParseInt(value.(string), 10, 64); e == nil {
			field, err = message.NewField(key, pValue, "")
			return
		}
		if pValue, e := strconv.ParseFloat(value.(string), 64); e == nil {
			field, err = message.NewField(key, pValue, "")
			return
		}
		if key == "timestamp" {
			if pValue, e := time.ParseInLocation(RFC3339Micro, value.(string), time.Local); e == nil {
				field, err = message.NewField("@Timestamp", pValue, "")
				return
			}
		}
		if key == "@Timestamp" {
			if pValue, e := time.ParseInLocation(RFC3339Micro, value.(string), time.Local); e == nil {
				field, err = message.NewField(key, pValue, "")
				return
			}
		}
		field, err = message.NewField(key, value.(string), "")

	case bool:
		field, err = message.NewField(key, value.(bool), "")

	default:
		err = fmt.Errorf("Unsupported JSON decode type: %s: %s", key, vtype)
	}
	return
}

// Decode decodes PipelinePack
func (decoder *UlogdDecoder) Decode(pack *pipeline.PipelinePack) (packs []*pipeline.PipelinePack, err error) {
	jDecoder := json.NewDecoder(strings.NewReader(pack.Message.GetPayload()))
	jDecoder.UseNumber()
	var jMessage interface{}
	if err = jDecoder.Decode(&jMessage); err != nil {
		return
	}

	matches := re.FindStringSubmatch(pack.Message.GetLogger())
	if matches == nil {
		err = errors.New("Logger has to be named *.Type")
		return
	}
	pack.Message.SetType(matches[1])

	for key, value := range jMessage.(map[string]interface{}) {
		var field *message.Field
		field, err = decodeJSON(key, value)
		if err != nil {
			return
		}
		pack.Message.AddField(field)
	}

	packs = []*pipeline.PipelinePack{pack}
	return
}

func init() {
	pipeline.RegisterPlugin("UlogdDecoder", func() interface{} {
		return new(UlogdDecoder)
	})
}
