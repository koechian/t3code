import { useEffect } from "react";

import { useDesktopUpdateState } from "../../state/desktopUpdate";
import { showDesktopUpdateInstalledToast } from "../desktopUpdate.toast";
import { recordLaunchedVersion } from "./desktopUpdateInstalled.logic";

// Remounting the app shell in the same window must not repeat the notice.
let checkedVersion: string | null = null;

function updatedVersionSinceLastLaunch(currentVersion: string): string | null {
  try {
    return recordLaunchedVersion(window.localStorage, currentVersion);
  } catch {
    // Storage can be unavailable; the notice is a convenience, so skip it.
    return null;
  }
}

/** Shows a toast on the first launch after the desktop app updated itself. */
export function DesktopUpdateInstalledNotice() {
  const currentVersion = useDesktopUpdateState()?.currentVersion ?? null;
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- records the launched version in localStorage and raises the imperative toast once per version
  useEffect(() => {
    if (currentVersion === null || checkedVersion === currentVersion) return;
    checkedVersion = currentVersion;
    const updatedTo = updatedVersionSinceLastLaunch(currentVersion);
    if (updatedTo !== null) showDesktopUpdateInstalledToast(window.desktopBridge, updatedTo);
  }, [currentVersion]);
  return null;
}
