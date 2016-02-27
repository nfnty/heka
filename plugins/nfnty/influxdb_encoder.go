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
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

// Divide nanosecond by these
const (
	DivNanoSecond  = 1e0
	DivMicroSecond = 1e3
	DivMilliSecond = 1e6
	DivSecond      = 1e9
	DivMinute      = DivSecond * 60
	DivHour        = DivMinute * 60
)

// InfluxdbEncoder is the backbone of the plugin
type InfluxdbEncoder struct {
	timestampDivision int64
}

// InfluxdbEncoderConfig contains user configuration
type InfluxdbEncoderConfig struct {
	timestampPrecision string `toml:"timestamp_precision"`
}

// ConfigStruct initializes the configuration with defaults
func (encoder *InfluxdbEncoder) ConfigStruct() interface{} {
	return &InfluxdbEncoderConfig{
		timestampPrecision: "ns",
	}
}

// Init initializes the plugin
func (encoder *InfluxdbEncoder) Init(config interface{}) (err error) {
	conf := config.(*InfluxdbEncoderConfig)
	switch conf.timestampPrecision {
	case "ns":
		encoder.timestampDivision = DivNanoSecond
	case "us":
		encoder.timestampDivision = DivMicroSecond
	case "ms":
		encoder.timestampDivision = DivMilliSecond
	case "s":
		encoder.timestampDivision = DivSecond
	case "m":
		encoder.timestampDivision = DivMinute
	case "h":
		encoder.timestampDivision = DivHour
	default:
		return errors.New("timestamp_precision has to be one of [ns, us, ms, s, m, h]")
	}
	return
}

func writeEscMeasure(buf *bytes.Buffer, str string) {
	for _, r := range str {
		if r == ',' || r == ' ' {
			buf.WriteRune('\\')
		}
		buf.WriteRune(r)
	}
}

func writeEscKey(buf *bytes.Buffer, str string) {
	for _, r := range str {
		if r == ',' || r == ' ' || r == '=' {
			buf.WriteRune('\\')
		}
		buf.WriteRune(r)
	}
}

func writeEscString(buf *bytes.Buffer, str string) {
	for _, r := range str {
		if r == '"' {
			buf.WriteRune('\\')
		}
		buf.WriteRune(r)
	}
}

func writeField(buf *bytes.Buffer, field *message.Field) (err error) {
	writeEscKey(buf, *field.Name)
	buf.WriteRune('=')

	fieldType := field.GetValueType()
	switch fieldType {
	case message.Field_INTEGER:
		values := field.GetValueInteger()
		if len(values) > 1 {
			err = fmt.Errorf("More than one value: integer: %s", *field.Name)
			return
		}
		buf.WriteString(strconv.FormatInt(values[0], 10))
		buf.WriteRune('i')

	case message.Field_DOUBLE:
		values := field.GetValueDouble()
		if len(values) > 1 {
			err = fmt.Errorf("More than one value: double: %s", *field.Name)
			return
		}
		buf.WriteString(strconv.FormatFloat(values[0], 'f', -1, 64))

	case message.Field_BOOL:
		values := field.GetValueBool()
		if len(values) > 1 {
			err = fmt.Errorf("More than one value: bool: %s", *field.Name)
			return
		}
		buf.WriteString(strconv.FormatBool(values[0]))

	case message.Field_STRING:
		values := field.GetValueString()
		if len(values) > 1 {
			err = fmt.Errorf("More than one value: string: %s", *field.Name)
			return
		}
		buf.WriteRune('"')
		writeEscString(buf, values[0])
		buf.WriteRune('"')

	default:
		err = fmt.Errorf("Unsupported field type: %s: %s",
			*field.Name, message.Field_ValueType_name[int32(fieldType)])
		return
	}
	return
}

// Encode encodes PipelinePack
func (encoder *InfluxdbEncoder) Encode(pack *pipeline.PipelinePack) (output []byte, err error) {
	msg := pack.Message
	buf := bytes.Buffer{}

	writeEscMeasure(&buf, *msg.Type)
	buf.WriteString(",Logger=")
	writeEscKey(&buf, *msg.Logger)
	buf.WriteRune(' ')

	first := true
	for _, field := range msg.Fields {
		if !first {
			buf.WriteRune(',')
		} else {
			first = false
		}
		if err = writeField(&buf, field); err != nil {
			return
		}
	}

	buf.WriteRune(' ')
	buf.WriteString(strconv.FormatInt(msg.GetTimestamp()/encoder.timestampDivision, 10))
	buf.WriteRune('\n')
	return buf.Bytes(), err
}

func init() {
	pipeline.RegisterPlugin("InfluxdbEncoder", func() interface{} {
		return new(InfluxdbEncoder)
	})
}
