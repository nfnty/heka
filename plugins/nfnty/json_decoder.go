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

// JSONDecoder is the backbone of the plugin
type JSONDecoder struct {
	timeLocation *time.Location
	timeKey      string
	timeLayout   string
	keysIgnore   []string
}

// JSONDecoderConfig contains user configuration
type JSONDecoderConfig struct {
	TimeLocation string   `toml:"time_location"`
	TimeKey      string   `toml:"time_key"`
	TimeLayout   string   `toml:"time_layout"`
	KeysIgnore   []string `toml:"keys_ignore"`
}

// ConfigStruct initializes the configuration with defaults
func (decoder *JSONDecoder) ConfigStruct() interface{} {
	return &JSONDecoderConfig{
		TimeLocation: time.Local.String(),
	}
}

// Init initializes the plugin
func (decoder *JSONDecoder) Init(config interface{}) (err error) {
	conf := config.(*JSONDecoderConfig)

	if decoder.timeLocation, err = time.LoadLocation(conf.TimeLocation); err != nil {
		return
	}

	if conf.TimeKey == "" {
		err = errors.New("time_key has to be defined")
		return
	}
	decoder.timeKey = conf.TimeKey

	if conf.TimeLayout == "" {
		err = errors.New("time_layout has to be defined")
		return
	}
	decoder.timeLayout = conf.TimeLayout

	decoder.keysIgnore = conf.KeysIgnore

	return
}

func (decoder *JSONDecoder) parseTimestamp(timestamp string) (t int64, err error) {
	pTime, err := time.ParseInLocation(decoder.timeLayout, timestamp, decoder.timeLocation)
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

func (decoder *JSONDecoder) parseJSON(key string, value interface{}) (field *message.Field, err error) {
	switch val := value.(type) {
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
	default:
		field, err = message.NewField(key, val, "")
	}
	return
}

var re = regexp.MustCompile("[^.]+\\.(.+)")

// Decode decodes PipelinePack
func (decoder *JSONDecoder) Decode(pack *pipeline.PipelinePack) (packs []*pipeline.PipelinePack, err error) {
	jDecoder := json.NewDecoder(strings.NewReader(pack.Message.GetPayload()))
	jDecoder.UseNumber()
	var jMessage interface{}
	if err = jDecoder.Decode(&jMessage); err != nil {
		return
	}

	matches := re.FindStringSubmatch(pack.Message.GetLogger())
	if matches == nil {
		err = errors.New("Logger has to be named +.Type+")
		return
	}
	pack.Message.SetType(matches[1])

	timeSet := false
	for key, value := range jMessage.(map[string]interface{}) {
		for _, f := range decoder.keysIgnore {
			if key == f {
				continue
			}
		}

		if key == decoder.timeKey {
			timeSet = true
			if val, ok := value.(string); ok {
				var pTime int64
				if pTime, err = decoder.parseTimestamp(val); err != nil {
					return
				}
				pack.Message.SetTimestamp(pTime)
			} else {
				err = fmt.Errorf("Timestamp is not a string (%T) \"%s\": %#v", value, key, value)
				return
			}
		} else {
			var field *message.Field
			if field, err = decoder.parseJSON(key, value); err != nil {
				return
			}
			pack.Message.AddField(field)
		}
	}

	if !timeSet {
		err = errors.New("time_key was not found")
		return
	}

	return []*pipeline.PipelinePack{pack}, err
}

func init() {
	pipeline.RegisterPlugin("JSONDecoder", func() interface{} {
		return new(JSONDecoder)
	})
}
