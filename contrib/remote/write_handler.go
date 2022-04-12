// Copyright 2022 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remote

import (
	"context"
	"net/http"
	"sort"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
)

var cacheSampleWindowSize = 20000
var samplesCache []sortableSample
var samplesChan = make(chan sortableSample, cacheSampleWindowSize)
var flushSamplesDuration = 30 * time.Second
var failedSamplesCount, samplesCount int64

type writeHandler struct {
	logger     log.Logger
	appendable storage.Appendable
}

// NewWriteHandler creates a http.Handler that accepts remote write requests and
// writes them to the provided appendable.
func NewWriteHandler(logger log.Logger, appendable storage.Appendable) http.Handler {
	h := &writeHandler{
		logger:     logger,
		appendable: appendable,
	}
	//TODO: It needs flush all data.
	go h.asyncAppend(context.Background())
	return h
}

func (h *writeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req, err := remote.DecodeWriteRequest(r.Body)
	if err != nil {
		level.Error(h.logger).Log("msg", "Error decoding remote write request", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = h.OutOfOrderWrite(r.Context(), req)
	switch err {
	case nil:
	case storage.ErrOutOfOrderSample, storage.ErrOutOfBounds, storage.ErrDuplicateSampleForTimestamp:
		// Indicated an out of order sample is a bad request to prevent retries.
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	default:
		level.Error(h.logger).Log("msg", "Error appending remote write", "err", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *writeHandler) OutOfOrderWrite(ctx context.Context, req *prompb.WriteRequest) (err error) {
	for _, ts := range req.Timeseries {
		labels := remote.LabelProtosToLabels(ts.Labels)
		//TODO(baokun.li): store exemplar
		for _, sample := range ts.Samples {
			samplesChan <- sortableSample{Labels: labels, Timestamp: sample.Timestamp, Value: sample.Value}
		}
	}
	return nil
}

func (h *writeHandler) asyncAppend(ctx context.Context) {
	ticker := time.NewTicker(flushSamplesDuration)
	for {
		select {
		case <-ctx.Done():
			h.appendTSDB(samplesCache)
			ticker.Stop()
			return
		case t := <-ticker.C:
			level.Info(h.logger).Log("timer write samples to TSDB", t.String())
			h.appendTSDB(samplesCache)
		case s := <-samplesChan:
			samplesCache = append(samplesCache, s)
			border := int(float64(cacheSampleWindowSize) * 0.3)
			if len(samplesCache) >= border {
				level.Debug(h.logger).Log("higher border case samples to TSDB", len(samplesCache))
				h.appendTSDB(samplesCache[:border])
				samplesCache = samplesCache[border:]
				ticker = time.NewTicker(flushSamplesDuration)
			}
		}
	}
}

func (h *writeHandler) appendTSDB(s []sortableSample) {
	if len(s) == 0 {
		return
	}
	var err error
	sort.Sort(sortableSamples(s))
	app := h.appendable.Appender(context.Background())
	defer app.Commit()
	
	for _, sample := range s {
		samplesCount++
		_, err = app.Append(0, sample.Labels, sample.Timestamp, sample.Value)
		if err != nil {
			level.Debug(h.logger).Log("append sample error", err)
			failedSamplesCount++
			continue
		}
	}
	if failedSamplesCount > 0 {
		failedRate := (float64(failedSamplesCount) / float64(samplesCount)) * 100
		level.Debug(h.logger).Log("failed sample count rate", failedRate)
	}
}

type sortableSample struct {
	Labels    labels.Labels
	Timestamp int64
	Value     float64
}

type sortableSamples []sortableSample

func (s sortableSamples) Len() int           { return len(s) }
func (s sortableSamples) Less(i, j int) bool { return s[i].Timestamp < s[j].Timestamp }
func (s sortableSamples) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
