import { ProviderDriverKind } from "@t3tools/contracts";
import { defineProviderClient } from "@t3tools/provider-core/client";

import { OmpSettings } from "./settings.ts";

// Paths traced from oh-my-pi's assets/icon.svg: the Pi glyph with a plug connector.
const GLYPH =
  "M12 8H108A2 2 0 0 1 110 10V18A2 2 0 0 1 108 20H12A2 2 0 0 1 10 18V10A2 2 0 0 1 12 8Z" +
  "M27 20H35A2 2 0 0 1 37 22V80A2 2 0 0 1 35 82H27A2 2 0 0 1 25 80V22A2 2 0 0 1 27 20Z" +
  "M77 20H85A2 2 0 0 1 87 22V57H75V22A2 2 0 0 1 77 20Z";
const CONNECTOR =
  "M74 55H88A3 3 0 0 1 91 58V68A3 3 0 0 1 88 71H74A3 3 0 0 1 71 68V58A3 3 0 0 1 74 55Z" +
  "M77 59H78A1 1 0 0 1 79 60V66A1 1 0 0 1 78 67H77A1 1 0 0 1 76 66V60A1 1 0 0 1 77 59Z" +
  "M83 59H84A1 1 0 0 1 85 60V66A1 1 0 0 1 84 67H83A1 1 0 0 1 82 66V60A1 1 0 0 1 83 59Z";
const DOTS = "M16 14A2 2 0 1 0 20 14A2 2 0 1 0 16 14ZM100 14A2 2 0 1 0 104 14A2 2 0 1 0 100 14Z";
const accent = { light: "#F97316", dark: "#F97316" };

export const ompClient = defineProviderClient({
  driverKind: ProviderDriverKind.make("omp"),
  label: "OMP",
  settingsSchema: OmpSettings,
  icon: {
    viewBox: "8 -7 104 104",
    fill: { light: "#0F0F0F", dark: "#F5F5F5" },
    paths: [
      { d: GLYPH },
      { d: CONNECTOR, fillRule: "evenodd", fill: accent },
      { d: DOTS, fill: accent },
    ],
  },
});
