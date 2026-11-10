package server

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"deepaicode2api/internal/deepai"
)

// handleResponses 是 /v1/responses 的入口。
//
// 两条路径：
//   - 目录里 api=openai-responses 的模型：原样透传到上游 /responses；
//   - 其余模型：把 Responses 请求翻译成 Chat Completions，再把结果
//     合成为 Responses 对象 / 事件流。
func (s *Server) handleResponses(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "仅支持 POST", "invalid_request_error")
		return
	}
	start := time.Now()

	raw, err := readBodyWithLimit(w, r)
	if err != nil {
		writeErr(w, http.StatusRequestEntityTooLarge, "请求体过大", "invalid_request_error")
		return
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error(), "invalid_request_error")
		return
	}

	model, modelOK := s.resolveModelOK(requestModelName(payload))
	if !modelOK {
		writeErr(w, http.StatusNotFound, s.unknownModelMessage(requestModelName(payload)), "invalid_request_error")
		return
	}
	payload["model"] = model.ID
	stream, _ := payload["stream"].(bool)
	affinity := affinityFor(r, payload)
	key := extractKey(r)
	promptLen := estimatePromptLen(payload)

	rls := s.newReqLog(key, nil, model.ID, "responses", stream, start, promptLen)
	if model.IsResponses() {
		s.passthroughResponses(w, r, payload, affinity, rls)
		return
	}

	chatPayload, err := responsesToChatPayload(payload, model.ID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error(), "invalid_request_error")
		return
	}
	if stream {
		s.streamResponsesAsChat(w, r, chatPayload, affinity, rls)
		return
	}
	s.blockingResponsesAsChat(w, r, chatPayload, affinity, rls)
}

// passthroughResponses 透传上游 /responses。
func (s *Server) passthroughResponses(w http.ResponseWriter, r *http.Request, payload map[string]any, affinity string, rl reqLog) {
	ctx, cancel := contextWithTimeout(r, s.requestTimeout())
	defer cancel()

	// 同上：上游缺省 stream 时按 SSE 返回，必须显式下发。
	payload["stream"] = rl.stream
	body, err := json.Marshal(payload)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "请求序列化失败", "invalid_request_error")
		return
	}
	resp, acct, err := s.openModel(ctx, deepai.PathResponses, body, affinity, s.requestGroup(r))
	rl.acct = acct
	if err != nil {
		rl.done(0, Usage{}, err)
		writeUpstreamErr(w, err)
		return
	}
	defer resp.Body.Close()

	if !rl.stream {
		rawResp, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		if err != nil {
			rl.done(0, Usage{}, err)
			writeErr(w, http.StatusBadGateway, "读取上游响应失败: "+err.Error(), "upstream_error")
			return
		}
		var out map[string]any
		if err := json.Unmarshal(rawResp, &out); err != nil {
			writeRawJSON(w, http.StatusOK, rawResp)
			return
		}
		rewriteResponsesModel(out, s.maskedModel(rl.model.ID))
		usage := parseUsage(out["usage"])
		outLen := responsesTextLen(out)
		encoded, _ := json.Marshal(out)
		rl.done(outLen, usage, nil)
		writeRawJSON(w, http.StatusOK, encoded)
		return
	}

	sw := newSSEWriter(w)
	var (
		usage  Usage
		outLen int
	)
	streamErr := forEachSSEFrame(resp.Body, func(f sseFrame) bool {
		if f.Event != "" {
			_, _ = w.Write([]byte("event: " + f.Event + "\n"))
			return true
		}
		if f.Data == "" {
			return true
		}
		if f.Data == sseDone {
			sw.done()
			return false
		}
		obj := f.Object
		if obj == nil {
			_, _ = w.Write([]byte("data: " + f.Data + "\n\n"))
			sw.flush()
			return true
		}
		rewriteResponsesModel(obj, rl.model)
		if respObj, ok := obj["response"].(map[string]any); ok {
			if u := parseUsage(respObj["usage"]); u.TotalTokens > 0 || u.Cost > 0 {
				usage = u
			}
		}
		outLen += responsesEventTextLen(obj)
		// 必须写出改写后的对象：直接写原始 data 会把 model 改写丢掉。
		if raw := jsonEvent(obj); raw != nil {
			sw.data(raw)
			return true
		}
		_, _ = w.Write([]byte("data: " + f.Data + "\n\n"))
		sw.flush()
		return true
	})
	if streamErr != nil {
		payload := jsonEvent(map[string]any{
			"type": "error", "message": streamErr.Error(),
		})
		if payload != nil {
			sw.event("error", payload)
		}
	}
	rl.done(outLen, usage, streamErr)
}

// blockingResponsesAsChat 用 Chat Completions 合成 Responses 响应。
func (s *Server) blockingResponsesAsChat(w http.ResponseWriter, r *http.Request, chatPayload map[string]any, affinity string, rl reqLog) {
	ctx, cancel := contextWithTimeout(r, s.requestTimeout())
	defer cancel()

	out, acct, err := s.callJSON(ctx, deepai.PathChat, chatPayload, affinity, s.requestGroup(r))
	rl.acct = acct
	if err != nil {
		rl.done(0, Usage{}, err)
		writeUpstreamErr(w, err)
		return
	}
	// Use masked model name
	maskedModel := s.maskedModel(rl.model.ID)
	respObj := chatToResponsesObject(out, maskedModel)
	usage := parseUsage(out["usage"])
	outLen := responsesTextLen(respObj)

	rl.done(outLen, usage, nil)
	writeJSON(w, http.StatusOK, respObj)
}

// streamResponsesAsChat 用 Chat Completions 流合成 Responses 事件流。
func (s *Server) streamResponsesAsChat(w http.ResponseWriter, r *http.Request, chatPayload map[string]any, affinity string, rl reqLog) {
	ctx, cancel := contextWithTimeout(r, s.requestTimeout())
	defer cancel()

	body, err := json.Marshal(chatPayload)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "请求序列化失败", "invalid_request_error")
		return
	}
	resp, acct, err := s.openModel(ctx, deepai.PathChat, body, affinity, s.requestGroup(r))
	rl.acct = acct
	if err != nil {
		rl.done(0, Usage{}, err)
		writeUpstreamErr(w, err)
		return
	}
	defer resp.Body.Close()

	sw := newSSEWriter(w)
	gen := newResponsesStreamGen(sw, rl.model)
	gen.created()

	var (
		usage  Usage
		outLen int
	)
	streamErr := forEachChatChunk(resp.Body, func(chunk map[string]any) bool {
		if u := parseUsage(chunk["usage"]); u.TotalTokens > 0 || u.Cost > 0 {
			usage = u
		}
		d, finish := chatChunkDelta(chunk)
		if d.Reasoning != "" {
			gen.reasoning(d.Reasoning)
			outLen += len([]rune(d.Reasoning))
		}
		if d.Content != "" {
			gen.textDelta(d.Content)
			outLen += len([]rune(d.Content))
		}
		for _, tc := range d.ToolCalls {
			gen.toolCall(tc.Index, tc.ID, tc.Name, tc.Args)
		}
		if finish != "" {
			gen.finishReason = finish
		}
		return true
	})

	if streamErr != nil {
		gen.fail(streamErr)
	}
	gen.complete(usage)
	rl.done(outLen, usage, streamErr)
}

// --- 工具函数 ---

var errNoMessages = &clientError{Message: "input 不能为空"}

type clientError struct{ Message string }

func (e *clientError) Error() string { return e.Message }

func boolOf(v any) bool {
	b, _ := v.(bool)
	return b
}

func strOf(v any) string {
	s, _ := v.(string)
	return s
}

// rewriteResponsesModel 把 Responses 对象 / 事件里的 model 改为对外模型名。
func rewriteResponsesModel(obj map[string]any, publicModel string) {
	if publicModel == "" {
		return
	}
	if _, ok := obj["model"]; ok {
		obj["model"] = publicModel
	}
	if resp, ok := obj["response"].(map[string]any); ok {
		rewriteResponsesModel(resp, publicModel)
	}
}

// responsesTextLen 统计 Responses 对象中的输出文本长度。
func responsesTextLen(obj map[string]any) int {
	total := 0
	if s, ok := obj["output_text"].(string); ok {
		total += len([]rune(s))
	}
	output, _ := obj["output"].([]any)
	for _, itemv := range output {
		item, ok := itemv.(map[string]any)
		if !ok {
			continue
		}
		switch strOf(item["type"]) {
		case "message":
			content, _ := item["content"].([]any)
			for _, cv := range content {
				c, ok := cv.(map[string]any)
				if ok {
					total += lenOfString(c["text"])
				}
			}
		case "reasoning":
			summary, _ := item["summary"].([]any)
			for _, sv := range summary {
				sm, ok := sv.(map[string]any)
				if ok {
					total += lenOfString(sm["text"])
				}
			}
		}
	}
	return total
}

// responsesEventTextLen 统计单个流事件的文本增量长度。
func responsesEventTextLen(obj map[string]any) int {
	if strOf(obj["type"]) == "response.output_text.delta" {
		return len([]rune(strOf(obj["delta"])))
	}
	if strOf(obj["type"]) == "response.reasoning_summary_text.delta" {
		return len([]rune(strOf(obj["delta"])))
	}
	return 0
}
