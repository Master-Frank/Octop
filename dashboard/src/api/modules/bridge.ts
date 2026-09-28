import { request } from "../request";
import type { ResolvedModel } from "../types";
import type { KnowledgeBase, KnowledgeCapability } from "./knowledgeBases";

export interface BridgeConnection {
  connection_id: string;
  peer_base_url: string;
  peer_username: string;
  display_name: string;
  notes: string | null;
  status: string;
  last_error: string | null;
  last_seen_at: number | null;
  auto_reconnect: boolean;
  created_at: number;
  updated_at: number;
  has_password: boolean;
}

export interface BridgeRemoteAgent {
  id: string;
  agent_id?: string;
  name?: string;
  description?: string | null;
  icon_url?: string | null;
  icon_name?: string | null;
  color?: string | null;
  state?: string | null;
  kind?: string | null;
  status?: string;
  bridge?: boolean;
  bridge_connection_id?: string;
  remote_agent_id?: string;
  [key: string]: unknown;
}

export interface BridgeProbeAgent {
  agent_id: string;
  name: string;
  description: string | null;
  icon_url: string | null;
  icon_name?: string | null;
  color?: string | null;
  state?: string | null;
  kind?: string | null;
}

export interface BridgeProbeResult {
  peer_base_url: string;
  peer_username: string;
  peer_display_name: string;
  agent_count: number;
  agents: BridgeProbeAgent[];
}

export const bridgeApi = {
  list: () => request<BridgeConnection[]>("/bridge/connections"),

  probe: (body: {
    peer_base_url: string;
    peer_username: string;
    password: string;
  }) =>
    request<BridgeProbeResult>("/bridge/probe", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  create: (body: {
    peer_base_url: string;
    peer_username: string;
    password: string;
    display_name: string;
    notes?: string;
    connect?: boolean;
  }) =>
    request<BridgeConnection>("/bridge/connections", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  get: (connectionId: string) =>
    request<BridgeConnection>(`/bridge/connections/${connectionId}`),

  connect: (connectionId: string) =>
    request<BridgeConnection>(`/bridge/connections/${connectionId}/connect`, {
      method: "POST",
    }),

  disconnect: (connectionId: string) =>
    request<BridgeConnection>(
      `/bridge/connections/${connectionId}/disconnect`,
      { method: "POST" },
    ),

  patch: (connectionId: string, body: { auto_reconnect?: boolean }) =>
    request<BridgeConnection>(`/bridge/connections/${connectionId}`, {
      method: "PATCH",
      body: JSON.stringify(body),
    }),

  remove: (connectionId: string) =>
    request<void>(`/bridge/connections/${connectionId}`, { method: "DELETE" }),

  listAgents: (connectionId: string) =>
    request<BridgeRemoteAgent[]>(`/bridge/connections/${connectionId}/agents`),

  listResolvedModels: (connectionId: string) =>
    request<ResolvedModel[]>(
      `/bridge/connections/${connectionId}/providers/resolved`,
    ),

  getActiveModel: (connectionId: string) =>
    request<{ provider_name: string; model: string }>(
      `/bridge/connections/${connectionId}/providers/active-model`,
    ),

  listKnowledgeBases: (connectionId: string) =>
    request<KnowledgeBase[]>(
      `/bridge/connections/${connectionId}/knowledge-bases`,
    ),

  getKnowledgeCapability: (connectionId: string) =>
    request<KnowledgeCapability>(
      `/bridge/connections/${connectionId}/knowledge-bases/capability`,
    ),
};
