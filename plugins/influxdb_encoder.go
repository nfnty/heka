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
#   Rob Miller (rmiller@mozilla.com)
#
# ***** END LICENSE BLOCK *****/

package plugins

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

type InfluxdbEncoder struct {
	timestamp_division int64
}

type InfluxdbEncoderConfig struct {
	TimestampPrecision string `toml:"timestamp_precision"`
}

func (ie *InfluxdbEncoder) ConfigStruct() interface{} {
	return &InfluxdbEncoderConfig{
		TimestampPrecision: "ns",
	}
}

func (ie *InfluxdbEncoder) Init(config interface{}) (err error) {
	conf := config.(*InfluxdbEncoderConfig)
	switch conf.TimestampPrecision {
	case "ns":
		ie.timestamp_division = 1e0
	case "us":
		ie.timestamp_division = 1e3
	case "ms":
		ie.timestamp_division = 1e6
	case "s":
		ie.timestamp_division = 1e9
	case "m":
		ie.timestamp_division = 60 * 1e9
	case "h":
		ie.timestamp_division = 60 * 60 * 1e9
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

func (ie *InfluxdbEncoder) Encode(pack *pipeline.PipelinePack) (output []byte, err error) {
	msg := pack.Message
	buf := bytes.Buffer{}

	writeEscMeasure(&buf, *msg.Type)
	buf.WriteString(",Logger=")
	writeEscKey(&buf, *msg.Logger)
	buf.WriteRune(' ')

	first := true
	for _, field := range msg.Fields {
		if first == false {
			buf.WriteRune(',')
		} else {
			first = false
		}
		writeEscKey(&buf, *field.Name)
		buf.WriteRune('=')

		field_type := field.GetValueType()
		switch field_type {
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
			writeEscString(&buf, values[0])
			buf.WriteRune('"')

		default:
			err = fmt.Errorf("Unsupported field type: %s: %s",
				*field.Name, message.Field_ValueType_name[int32(field_type)])
			return
		}
	}

	buf.WriteRune(' ')
	buf.WriteString(strconv.FormatInt(msg.GetTimestamp()/ie.timestamp_division, 10))
	buf.WriteRune('\n')
	return buf.Bytes(), err
}

func init() {
	pipeline.RegisterPlugin("InfluxdbEncoder", func() interface{} {
		return new(InfluxdbEncoder)
	})
}
