import { useTranslation } from "react-i18next";
import { Tooltip } from "antd";
import styles from "../index.module.less";

interface RemoteExpertHintProps {
  agent?: {
    bridge?: boolean | null;
    bridge_connection_name?: string | null;
  } | null;
  /** Force show without checking ``agent.bridge`` (title bar). */
  show?: boolean;
  connectionName?: string | null;
}

/** Marks a sidebar / picker row as a remote Bridge shadow expert. */
export default function RemoteExpertHint({
  agent,
  show,
  connectionName,
}: RemoteExpertHintProps) {
  const { t } = useTranslation();
  const visible = show ?? Boolean(agent?.bridge);
  if (!visible) return null;
  const name = connectionName ?? agent?.bridge_connection_name?.trim() ?? "";
  const tip = name
    ? t("chat.remoteExpert.banner", { name })
    : t("chat.remoteExpert.flag");
  const label = t("chat.remoteExpert.flag");
  return (
    <Tooltip title={tip} mouseEnterDelay={0.35}>
      <span
        className={styles.remoteExpertFlag}
        aria-label={tip}
        onClick={(event) => event.stopPropagation()}
      >
        {label}
      </span>
    </Tooltip>
  );
}
