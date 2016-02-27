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
	"strings"
	"time"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

// RFC3339Micro is a time layout without timezone
const RFC3339Micro string = "2006-01-02T15:04:05.999999"

var re = regexp.MustCompile(".*\\.(.*)")

// UlogdDecoder is the backbone of the plugin
type UlogdDecoder struct {
	timeLocation *time.Location
}

// UlogdDecoderConfig contains user configuration
type UlogdDecoderConfig struct {
	TimeLocation string `toml:"time_location"`
}

// ConfigStruct initializes the configuration with defaults
func (decoder *UlogdDecoder) ConfigStruct() interface{} {
	return &UlogdDecoderConfig{
		TimeLocation: time.Local.String(),
	}
}

// Init initializes the plugin
func (decoder *UlogdDecoder) Init(config interface{}) (err error) {
	conf := config.(*UlogdDecoderConfig)
	decoder.timeLocation, err = time.LoadLocation(conf.TimeLocation)
	return
}

func (decoder *UlogdDecoder) parseTimestamp(timestamp string) (t int64, err error) {
	pTime, err := time.ParseInLocation(RFC3339Micro, timestamp, decoder.timeLocation)
	if err != nil {
		err = errors.New("Failed to parse timestamp")
		return
	}
	if pTime.IsZero() {
		err = errors.New("Timestamp is zero")
		return
	}
	return pTime.UnixNano(), err
}

func (decoder *UlogdDecoder) parseJSON(key string, value interface{}) (field *message.Field, err error) {
	switch val := value.(type) {
	case string:
		if key == "@Timestamp" || key == "timestamp" {
			var pValue int64
			if pValue, err = decoder.parseTimestamp(val); err != nil {
				return
			}
			field, err = message.NewField("@Timestamp", pValue, "")
			return
		}
		field, err = message.NewField(key, val, "")

	case json.Number:
		if pValue, e := val.Int64(); e == nil {
			field, err = message.NewField(key, pValue, "")
			return
		}
		if pValue, e := val.Float64(); e == nil {
			field, err = message.NewField(key, pValue, "")
			return
		}
		err = fmt.Errorf("Failed to decode json.Number: %s: %s", key, val)

	case bool:
		field, err = message.NewField(key, val, "")

	default:
		err = fmt.Errorf("Unsupported JSON decode type (%T) \"%s\": %#v", val, key, val)
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
		if field, err = decoder.parseJSON(key, value); err != nil {
			return
		}
		pack.Message.AddField(field)
	}

	return []*pipeline.PipelinePack{pack}, err
}

func init() {
	pipeline.RegisterPlugin("UlogdDecoder", func() interface{} {
		return new(UlogdDecoder)
	})
}
