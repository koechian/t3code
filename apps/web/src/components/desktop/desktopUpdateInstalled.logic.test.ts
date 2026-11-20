import { describe, expect, it } from "vite-plus/test";

import {
  LAST_LAUNCHED_VERSION_STORAGE_KEY,
  installedUpdateVersion,
  recordLaunchedVersion,
} from "./desktopUpdateInstalled.logic";

function memoryStorage(initial: Record<string, string> = {}): Storage {
  const values = new Map(Object.entries(initial));
  return {
    get length() {
      return values.size;
    },
    clear: () => values.clear(),
    getItem: (key) => values.get(key) ?? null,
    key: (index) => [...values.keys()][index] ?? null,
    removeItem: (key) => {
      values.delete(key);
    },
    setItem: (key, value) => {
      values.set(key, value);
    },
  };
}

describe("installedUpdateVersion", () => {
  it("does not report the first recorded launch", () => {
    expect(installedUpdateVersion(null, "0.1.10")).toBeNull();
  });

  it("does not report a relaunch on the same version", () => {
    expect(installedUpdateVersion("0.1.10", "0.1.10")).toBeNull();
  });

  it("reports the version a launch changed to", () => {
    expect(installedUpdateVersion("0.1.10-local.202611171553", "0.1.10-local.202611201012")).toBe(
      "0.1.10-local.202611201012",
    );
  });
});

describe("recordLaunchedVersion", () => {
  it("records the version and reports it once", () => {
    const storage = memoryStorage({ [LAST_LAUNCHED_VERSION_STORAGE_KEY]: "0.1.9" });

    expect(recordLaunchedVersion(storage, "0.1.10")).toBe("0.1.10");
    expect(storage.getItem(LAST_LAUNCHED_VERSION_STORAGE_KEY)).toBe("0.1.10");
    expect(recordLaunchedVersion(storage, "0.1.10")).toBeNull();
  });
});
