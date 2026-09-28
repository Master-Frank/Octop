import { useCallback, useEffect, useState } from "react";
import {
  Avatar,
  Button,
  Drawer,
  Form,
  Input,
  Popconfirm,
  Spin,
  Tag,
} from "antd";
import { message } from "@/utils/antdMessage";
import {
  Activity,
  Bot,
  Cable,
  Globe,
  Link2,
  Lock,
  Plus,
  RefreshCw,
  Share2,
  Tag as TagIcon,
  Trash2,
  Unplug,
  User,
} from "lucide-react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";

import { EmptyState } from "../../../components/EmptyState";
import { useAgent } from "../../../context/AgentContext";
import { apiErrorMessage } from "../../../utils/apiError";
import {
  bridgeApi,
  type BridgeConnection,
  type BridgeProbeAgent,
  type BridgeProbeResult,
  type BridgeRemoteAgent,
} from "../../../api/modules/bridge";
import { TabPanelHeader } from "./TabPanelHeader";
import styles from "./BridgeSettings.module.less";

const FIELD_ICON = {
  size: 16 as const,
  style: { color: "var(--fn-text-tertiary)" },
};

function statusColor(status: string): string {
  if (status === "connected") return "success";
  if (status === "connecting" || status === "error") return "warning";
  return "default";
}

function AgentAvatar({
  name,
  iconUrl,
  color,
}: {
  name: string;
  iconUrl?: string | null;
  color?: string | null;
}) {
  if (iconUrl) {
    return <Avatar size={40} src={iconUrl} alt={name} />;
  }
  const initial = (name || "?").trim().charAt(0).toUpperCase() || "?";
  return (
    <Avatar
      size={40}
      style={{
        background: color || "var(--fn-color-brand, #e85d75)",
        color: "#fff",
      }}
    >
      {initial}
    </Avatar>
  );
}

function KindTag({ kind }: { kind?: string | null }) {
  const { t } = useTranslation();
  const isTeam = kind === "team";
  return (
    <Tag color={isTeam ? "purple" : "blue"} style={{ marginInlineEnd: 0 }}>
      {isTeam
        ? t("advancedSettings.bridge.kindTeam")
        : t("advancedSettings.bridge.kindExpert")}
    </Tag>
  );
}

function AgentListItem({
  agentId,
  name,
  description,
  iconUrl,
  color,
  kind,
  onClick,
}: {
  agentId: string;
  name: string;
  description?: string | null;
  iconUrl?: string | null;
  color?: string | null;
  kind?: string | null;
  onClick?: () => void;
}) {
  const body = (
    <>
      <AgentAvatar name={name} iconUrl={iconUrl} color={color} />
      <div className={styles.probeBody}>
        <div className={styles.probeNameRow}>
          <div className={styles.probeName}>{name}</div>
          <KindTag kind={kind} />
        </div>
        {description?.trim() ? (
          <div className={styles.probeDesc}>{description}</div>
        ) : null}
      </div>
    </>
  );
  if (onClick) {
    return (
      <li>
        <button
          type="button"
          className={styles.connectedAgentBtn}
          aria-label={name}
          data-agent-id={agentId}
          onClick={onClick}
        >
          {body}
        </button>
      </li>
    );
  }
  return (
    <li className={styles.probeItem} data-agent-id={agentId}>
      {body}
    </li>
  );
}

function ProbeAgentList({ agents }: { agents: BridgeProbeAgent[] }) {
  const { t } = useTranslation();
  if (agents.length === 0) {
    return (
      <p className={styles.probeEmpty}>
        {t("advancedSettings.bridge.noAgents")}
      </p>
    );
  }
  return (
    <ul className={styles.probeList}>
      {agents.map((agent) => (
        <AgentListItem
          key={agent.agent_id}
          agentId={agent.agent_id}
          name={agent.name}
          description={agent.description}
          iconUrl={agent.icon_url}
          color={agent.color}
          kind={agent.kind}
        />
      ))}
    </ul>
  );
}

export default function BridgeSettingsPanel() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { refresh: refreshAgents } = useAgent();
  const [loading, setLoading] = useState(true);
  const [rows, setRows] = useState<BridgeConnection[]>([]);
  const [agentsByConn, setAgentsByConn] = useState<
    Record<string, BridgeRemoteAgent[]>
  >({});
  const [createOpen, setCreateOpen] = useState(false);
  const [creating, setCreating] = useState(false);
  const [probing, setProbing] = useState(false);
  const [probeResult, setProbeResult] = useState<BridgeProbeResult | null>(
    null,
  );
  const [form] = Form.useForm();

  const closeCreateDrawer = () => {
    setCreateOpen(false);
    setProbeResult(null);
    form.resetFields();
  };

  const reload = useCallback(async () => {
    setLoading(true);
    try {
      const list = await bridgeApi.list();
      setRows(list);
      const next: Record<string, BridgeRemoteAgent[]> = {};
      await Promise.all(
        list
          .filter((c) => c.status === "connected")
          .map(async (c) => {
            try {
              next[c.connection_id] = await bridgeApi.listAgents(
                c.connection_id,
              );
            } catch {
              next[c.connection_id] = [];
            }
          }),
      );
      setAgentsByConn(next);
    } catch (err) {
      message.error(
        apiErrorMessage(err, t("advancedSettings.bridge.loadFailed"), t),
      );
    } finally {
      setLoading(false);
    }
  }, [t]);

  useEffect(() => {
    void reload();
  }, [reload]);

  const onCreate = async () => {
    try {
      const values = await form.validateFields();
      setCreating(true);
      await bridgeApi.create({
        peer_base_url: values.peer_base_url,
        peer_username: values.peer_username,
        password: values.password,
        display_name: values.display_name,
        notes: values.notes || undefined,
        connect: true,
      });
      message.success(t("advancedSettings.bridge.created"));
      closeCreateDrawer();
      await reload();
      await refreshAgents({ silent: true, force: true });
    } catch (err) {
      if (err && typeof err === "object" && "errorFields" in err) return;
      message.error(
        apiErrorMessage(err, t("advancedSettings.bridge.createFailed"), t),
      );
    } finally {
      setCreating(false);
    }
  };

  const onProbe = async () => {
    try {
      const values = await form.validateFields([
        "peer_base_url",
        "peer_username",
        "password",
      ]);
      setProbing(true);
      const result = await bridgeApi.probe({
        peer_base_url: values.peer_base_url,
        peer_username: values.peer_username,
        password: values.password,
      });
      setProbeResult(result);
      message.success(
        t("advancedSettings.bridge.probeOk", { count: result.agent_count }),
      );
    } catch (err) {
      if (err && typeof err === "object" && "errorFields" in err) return;
      setProbeResult(null);
      message.error(
        apiErrorMessage(err, t("advancedSettings.bridge.probeFailed"), t),
      );
    } finally {
      setProbing(false);
    }
  };

  const onDelete = async (row: BridgeConnection) => {
    try {
      await bridgeApi.remove(row.connection_id);
      message.success(t("advancedSettings.bridge.deleted"));
      await reload();
      await refreshAgents({ silent: true, force: true });
    } catch (err) {
      message.error(
        apiErrorMessage(err, t("advancedSettings.bridge.deleteFailed"), t),
      );
    }
  };

  return (
    <>
      <TabPanelHeader
        icon={<Share2 size={18} strokeWidth={1.8} />}
        title={t("advancedSettings.bridge.title")}
        description={t("advancedSettings.bridge.description")}
        actions={
          <>
            <Button
              icon={<RefreshCw size={14} />}
              onClick={() => void reload()}
            >
              {t("common.refresh")}
            </Button>
            <Button
              type="primary"
              icon={<Plus size={14} />}
              onClick={() => setCreateOpen(true)}
            >
              {t("advancedSettings.bridge.add")}
            </Button>
          </>
        }
      />

      {loading ? (
        <div className={styles.loading}>
          <Spin />
        </div>
      ) : rows.length === 0 ? (
        <EmptyState
          variant="mascot"
          title={t("advancedSettings.bridge.emptyTitle")}
          description={t("advancedSettings.bridge.emptyDesc")}
        />
      ) : (
        <div className={styles.list}>
          {rows.map((row) => (
            <section key={row.connection_id} className={styles.card}>
              <header className={styles.cardHead}>
                <div className={styles.cardTitle}>
                  <Link2 size={16} />
                  <strong>{row.display_name}</strong>
                  <Tag color={statusColor(row.status)}>
                    {t(`advancedSettings.bridge.status.${row.status}`, {
                      defaultValue: row.status,
                    })}
                  </Tag>
                </div>
                <div className={styles.cardActions}>
                  {row.status === "connected" ? (
                    <Button
                      size="small"
                      icon={<Unplug size={14} />}
                      onClick={async () => {
                        try {
                          await bridgeApi.disconnect(row.connection_id);
                          await reload();
                          await refreshAgents({ silent: true, force: true });
                        } catch (err) {
                          message.error(
                            apiErrorMessage(
                              err,
                              t("advancedSettings.bridge.loadFailed"),
                              t,
                            ),
                          );
                        }
                      }}
                    >
                      {t("advancedSettings.bridge.disconnect")}
                    </Button>
                  ) : (
                    <Button
                      size="small"
                      type="primary"
                      icon={<Cable size={14} />}
                      onClick={async () => {
                        try {
                          await bridgeApi.connect(row.connection_id);
                          await reload();
                          await refreshAgents({ silent: true, force: true });
                        } catch (err) {
                          message.error(
                            apiErrorMessage(
                              err,
                              t("advancedSettings.bridge.createFailed"),
                              t,
                            ),
                          );
                        }
                      }}
                    >
                      {t("advancedSettings.bridge.connect")}
                    </Button>
                  )}
                  <Popconfirm
                    title={t("advancedSettings.bridge.deleteConfirmTitle")}
                    description={t(
                      "advancedSettings.bridge.deleteConfirmDesc",
                      { name: row.display_name },
                    )}
                    okText={t("common.delete")}
                    cancelText={t("common.cancel")}
                    okButtonProps={{ danger: true }}
                    onConfirm={() => void onDelete(row)}
                  >
                    <Button
                      size="small"
                      danger
                      icon={<Trash2 size={14} />}
                      aria-label={t("advancedSettings.bridge.delete")}
                    />
                  </Popconfirm>
                </div>
              </header>
              <p className={styles.meta}>
                {row.peer_base_url} · {row.peer_username}
              </p>
              {row.notes ? <p className={styles.meta}>{row.notes}</p> : null}
              {row.last_error ? (
                <p className={styles.metaError}>{row.last_error}</p>
              ) : null}
              {(agentsByConn[row.connection_id] || []).length > 0 ? (
                <ul className={styles.probeList}>
                  {agentsByConn[row.connection_id].map((agent) => (
                    <AgentListItem
                      key={agent.id}
                      agentId={agent.id}
                      name={String(
                        agent.name || agent.remote_agent_id || agent.id,
                      )}
                      description={
                        typeof agent.description === "string"
                          ? agent.description
                          : null
                      }
                      iconUrl={
                        typeof agent.icon_url === "string"
                          ? agent.icon_url
                          : null
                      }
                      color={
                        typeof agent.color === "string" ? agent.color : null
                      }
                      kind={typeof agent.kind === "string" ? agent.kind : null}
                      onClick={() =>
                        navigate(`/chat/${encodeURIComponent(agent.id)}`)
                      }
                    />
                  ))}
                </ul>
              ) : row.status === "connected" ? (
                <p className={styles.meta}>
                  {t("advancedSettings.bridge.noAgents")}
                </p>
              ) : null}
            </section>
          ))}
        </div>
      )}

      <Drawer
        title={t("advancedSettings.bridge.add")}
        open={createOpen}
        onClose={closeCreateDrawer}
        width={460}
        placement="right"
        destroyOnHidden
        className={styles.createDrawer}
        footer={
          <div className={styles.drawerFooter}>
            <Button onClick={closeCreateDrawer}>{t("common.cancel")}</Button>
            <Button
              icon={<Activity size={14} />}
              loading={probing}
              onClick={() => void onProbe()}
            >
              {t("advancedSettings.bridge.probe")}
            </Button>
            <Button
              type="primary"
              loading={creating}
              icon={<Cable size={14} />}
              onClick={() => void onCreate()}
            >
              {t("advancedSettings.bridge.addConfirm")}
            </Button>
          </div>
        }
      >
        <p className={styles.drawerHint}>
          {t("advancedSettings.bridge.drawerHint")}
        </p>
        <Form
          form={form}
          layout="vertical"
          requiredMark={false}
          className={styles.createForm}
        >
          <div className={styles.createSection}>
            <div className={styles.createSectionTitle}>
              {t("advancedSettings.bridge.sectionIdentity")}
            </div>
            <Form.Item
              name="display_name"
              label={t("advancedSettings.bridge.displayName")}
              extra={t("advancedSettings.bridge.displayNameHint")}
              rules={[
                {
                  required: true,
                  whitespace: true,
                  message: t("advancedSettings.bridge.displayNameRequired"),
                },
                { max: 64 },
              ]}
            >
              <Input
                prefix={<TagIcon {...FIELD_ICON} />}
                placeholder={t(
                  "advancedSettings.bridge.displayNamePlaceholder",
                )}
                maxLength={64}
                autoFocus
              />
            </Form.Item>
            <Form.Item
              name="notes"
              label={t("advancedSettings.bridge.notes")}
              rules={[{ max: 500 }]}
            >
              <Input.TextArea
                placeholder={t("advancedSettings.bridge.notesPlaceholder")}
                autoSize={{ minRows: 2, maxRows: 4 }}
                maxLength={500}
              />
            </Form.Item>
          </div>

          <div className={styles.createSection}>
            <div className={styles.createSectionTitle}>
              {t("advancedSettings.bridge.sectionRemote")}
            </div>
            <Form.Item
              name="peer_base_url"
              label={t("advancedSettings.bridge.peerUrl")}
              rules={[
                {
                  required: true,
                  whitespace: true,
                  message: t("advancedSettings.bridge.peerUrlRequired"),
                },
              ]}
            >
              <Input
                prefix={<Globe {...FIELD_ICON} />}
                placeholder={t("advancedSettings.bridge.peerUrlPlaceholder")}
                autoComplete="url"
              />
            </Form.Item>
            <Form.Item
              name="peer_username"
              label={t("advancedSettings.bridge.username")}
              rules={[
                {
                  required: true,
                  whitespace: true,
                  message: t("advancedSettings.bridge.usernameRequired"),
                },
              ]}
            >
              <Input
                prefix={<User {...FIELD_ICON} />}
                autoComplete="username"
              />
            </Form.Item>
            <Form.Item
              name="password"
              label={t("advancedSettings.bridge.password")}
              rules={[
                {
                  required: true,
                  message: t("advancedSettings.bridge.passwordRequired"),
                },
              ]}
            >
              <Input.Password
                prefix={<Lock {...FIELD_ICON} />}
                autoComplete="current-password"
              />
            </Form.Item>
          </div>
        </Form>

        {probeResult ? (
          <div className={styles.probePanel}>
            <div className={styles.probePanelHead}>
              <Bot size={16} strokeWidth={1.8} />
              <span>
                {t("advancedSettings.bridge.probeResultTitle", {
                  name: probeResult.peer_display_name,
                  count: probeResult.agent_count,
                })}
              </span>
            </div>
            <ProbeAgentList agents={probeResult.agents} />
          </div>
        ) : null}
      </Drawer>
    </>
  );
}
