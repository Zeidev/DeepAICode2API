package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"deepaicode2api/internal/registry"
	"deepaicode2api/internal/store"
)

// --- 控制台管理接口 ---

// sanitizedSettings 返回可下发给前端的设置（管理密码不外泄）。
func sanitizedSettings(st store.Settings) map[string]any {
	return map[string]any{
		"model_aliases":       st.ModelAliases,
		"request_timeout_sec": st.RequestTimeout,
		"max_retries":         st.MaxRetries,
		"log_keep":            st.LogKeep,
		"cors_origin":         st.CORSOrigin,
		"fallback_model":      st.FallbackModel,
		"model_mask":          st.ModelMask,
		"strict_model":        st.StrictModel,
		"lane":                st.Lane,
		"admin_password_set":  st.AdminPassword != "",
	}
}

// --- 控制台 API ---

func (s *Server) handleAdminState(w http.ResponseWriter, r *http.Request) {
	// 兼容端点：默认仍下发全部账号（外部脚本可能在用）。控制台已改用
	// /admin/api/overview + /admin/api/accounts（分页）；需要小响应时加 ?accounts=0。
	var safe []map[string]any
	if r.URL.Query().Get("accounts") != "0" {
		accts := s.Store.Accounts()
		safe = make([]map[string]any, 0, len(accts))
		for _, a := range accts {
			safe = append(safe, accountView(a))
		}
	}
	safeKeys := safeKeyList(s.Store.Keys())
	catalog := s.catalog()
	writeJSON(w, http.StatusOK, map[string]any{
		"accounts":       safe,
		"keys":           safeKeys,
		"models":         registry.List(catalog, s.Store.Settings().ModelAliases),
		"default_model":  s.Store.DefaultModel(),
		"models_updated": s.Store.ModelsUpdated(),
		"machine_id":     shortMachine(s.Store.MachineID()),
		"settings":       sanitizedSettings(s.Store.Settings()),
		"stats":          s.Store.Stats(),
		"upstream":       s.Up.Origin,
	})
}

func (s *Server) handleAdminKeys(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"keys": safeKeyList(s.Store.Keys())})
	case http.MethodPost:
		var body struct {
			Action string `json:"action"`
			Name   string `json:"name"`
			Group  string `json:"group"`
			Key    string `json:"key"`
		}
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body)
		// action=set-group：只改绑定分组（保持密钥值不变）；其余 / 空值仍为新建。
		if body.Action == "set-group" {
			key := s.resolveKeyRef(strings.TrimSpace(body.Key))
			if key == "" {
				writeErr(w, http.StatusBadRequest, "缺少 key", "invalid_request_error")
				return
			}
			ok := s.Store.SetKeyGroup(key, body.Group)
			_ = s.Store.Save()
			writeJSON(w, http.StatusOK, map[string]any{"ok": ok, "group": s.Store.KeyGroup(key)})
			return
		}
		k := s.Store.AddKey(body.Name, body.Group)
		_ = s.Store.Save()
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "key": k.Key})
	case http.MethodDelete:
		q := r.URL.Query()
		if ref := q.Get("key"); ref != "" {
			_ = s.Store.DeleteKey(s.resolveKeyRef(ref))
		}
		if ref := q.Get("toggle"); ref != "" {
			_ = s.Store.ToggleKey(s.resolveKeyRef(ref))
		}
		_ = s.Store.Save()
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	default:
		writeErr(w, http.StatusMethodNotAllowed, "不支持的方法", "invalid_request_error")
	}
}

func (s *Server) handleAdminLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodDelete {
		s.Store.ClearLogs()
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}
	// 支持 limit / since / endpoint / account / min_status / q。
	// 无参数时行为与旧版一致（最近 limit 条，默认 200）。
	f := logFilterFromQuery(r)
	writeJSON(w, http.StatusOK, map[string]any{
		"logs":  s.Store.QueryLogs(f),
		"limit": f.Limit,
		"filter": map[string]any{
			"since": f.Since, "endpoint": f.Endpoint, "account": f.Account,
			"min_status": f.MinStatus, "q": f.Keyword,
		},
	})
}

func (s *Server) handleAdminStats(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodDelete {
		s.Store.ClearStats()
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}
	writeJSON(w, http.StatusOK, s.Store.Stats())
}

func (s *Server) handleAdminSettings(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusOK, sanitizedSettings(s.Store.Settings()))
		return
	}
	var body struct {
		ModelAliases   map[string]string `json:"model_aliases"`
		RequestTimeout *int              `json:"request_timeout_sec"`
		MaxRetries     *int              `json:"max_retries"`
		LogKeep        *int              `json:"log_keep"`
		CORSOrigin     *string           `json:"cors_origin"`
		AdminPassword  *string           `json:"admin_password"`
		FallbackModel  *string           `json:"fallback_model"`
		StrictModel    *bool             `json:"strict_model"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败", "invalid_request_error")
		return
	}
	s.Store.UpdateSettings(func(st *store.Settings) {
		if body.ModelAliases != nil {
			st.ModelAliases = body.ModelAliases
		}
		if body.RequestTimeout != nil && *body.RequestTimeout > 0 {
			st.RequestTimeout = *body.RequestTimeout
		}
		if body.MaxRetries != nil && *body.MaxRetries > 0 {
			st.MaxRetries = *body.MaxRetries
		}
		if body.LogKeep != nil && *body.LogKeep > 0 {
			st.LogKeep = *body.LogKeep
		}
		if body.CORSOrigin != nil {
			st.CORSOrigin = *body.CORSOrigin
		}
		if body.AdminPassword != nil {
			st.AdminPassword = strings.TrimSpace(*body.AdminPassword)
		}
		if body.FallbackModel != nil {
			st.FallbackModel = strings.TrimSpace(*body.FallbackModel)
		}
		if body.ModelMask != nil {
			st.ModelMask = strings.TrimSpace(*body.ModelMask)
		}
		if body.ModelMask != nil {
			st.ModelMask = strings.TrimSpace(*body.ModelMask)
		}
		if body.StrictModel != nil {
			st.StrictModel = *body.StrictModel
		}
	})
	_ = s.Store.Save()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "settings": sanitizedSettings(s.Store.Settings())})
}

// handleAdminModels 刷新模型目录；GET 返回缓存。
func (s *Server) handleAdminModels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		catalog := s.Store.Models()
		writeJSON(w, http.StatusOK, map[string]any{
			"models":         registry.List(catalog, s.Store.Settings().ModelAliases),
			"default_model":  s.Store.DefaultModel(),
			"models_updated": s.Store.ModelsUpdated(),
		})
		return
	}
	models, err := s.RefreshModels(r.Context())
	if err != nil {
		writeErr(w, http.StatusBadGateway, "刷新模型目录失败: "+err.Error(), "upstream_error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "count": len(models), "default_model": s.Store.DefaultModel(),
	})
}

// safeKeyList 把密钥列表转成控制台下发的视图（不含完整密钥）。
func safeKeyList(keys []*store.APIKey) []map[string]any {
	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, map[string]any{
			"key": mask(k.Key), "name": k.Name, "group": k.Group, "enabled": k.Enabled,
			"created_at": k.CreatedAt, "last_used": k.LastUsed, "requests": k.Requests,
		})
	}
	return out
}

// mask 对敏感串做脱敏，仅保留首尾片段。
func mask(s string) string {
	if len(s) <= 10 {
		if s == "" {
			return ""
		}
		return s[:1] + "***"
	}
	return s[:6] + "..." + s[len(s)-4:]
}

// shortMachine 只展示机器指纹前 12 位。
func shortMachine(id string) string {
	if len(id) <= 12 {
		return id
	}
	return id[:12]
}

// maskMachine 返回机器指纹的可读摘要。
//
// 账号级 machine 是原始 UUID，完整值不上控制台（它决定上游怎么发额度）。
func maskMachine(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if len(raw) <= 8 {
		return raw
	}
	return raw[:8] + "…"
}
