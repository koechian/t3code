export const LAST_LAUNCHED_VERSION_STORAGE_KEY = "t3code:desktop:last-launched-version";

/**
 * The version this launch was updated to, or null. The first launch with no recorded version is
 * not an update, so it only records the version.
 */
export function installedUpdateVersion(
  lastLaunchedVersion: string | null,
  currentVersion: string,
): string | null {
  if (lastLaunchedVersion === null || lastLaunchedVersion === currentVersion) return null;
  return currentVersion;
}

/** Records this launch's version and returns the version it was updated to, if any. */
export function recordLaunchedVersion(storage: Storage, currentVersion: string): string | null {
  const updatedTo = installedUpdateVersion(
    storage.getItem(LAST_LAUNCHED_VERSION_STORAGE_KEY),
    currentVersion,
  );
  storage.setItem(LAST_LAUNCHED_VERSION_STORAGE_KEY, currentVersion);
  return updatedTo;
}
