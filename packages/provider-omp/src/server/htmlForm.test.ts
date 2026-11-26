// @effect-diagnostics-next-line nodeBuiltinImport:off -- Pure tests read their recorded frames synchronously.
import * as NodeFS from "node:fs";

import * as Schema from "effect/Schema";
import { describe, expect, it, vi } from "vite-plus/test";

import { ompFormData, ompFormQuestions, parseOmpFormHtml, type OmpParsedForm } from "./htmlForm.ts";

const FixtureLine = Schema.Struct({
  frame: Schema.Record(Schema.String, Schema.Unknown),
  source: Schema.String,
});
const decodeFixtureLine = Schema.decodeUnknownSync(Schema.fromJsonString(FixtureLine));

const fixtureLines = NodeFS.readFileSync(
  new URL("./__fixtures__/htmlForm/forms.jsonl", import.meta.url),
  "utf8",
)
  .trim()
  .split("\n")
  .map((line) => decodeFixtureLine(line));

function fixtureFrame(source: string) {
  const line = fixtureLines.find((candidate) => candidate.source.endsWith(source));
  if (line === undefined) throw new Error(`no fixture line ${source}`);
  return line.frame;
}

function formHtml(source: string): string {
  const html = fixtureFrame(source)["html"];
  if (typeof html !== "string") throw new Error(`${source} is not a form request`);
  return html;
}

function parsedForm(html: string): OmpParsedForm {
  const result = parseOmpFormHtml(html);
  if (!result.ok) throw new Error(result.reason);
  return result.form;
}

describe("ompFormQuestions", () => {
  it.each([
    {
      source: "ask-tool-form-shapes.jsonl:37",
      questions: [
        {
          id: "colour",
          header: "Colour",
          question: "Which colour do you prefer?",
          options: [
            { label: "Red", description: "Red", value: "red" },
            { label: "Green", description: "Default", value: "green" },
            { label: "Blue", description: "Blue", value: "blue" },
          ],
          allowCustomAnswer: true,
          required: false,
        },
      ],
    },
    {
      source: "ask-tool-form-shapes.jsonl:85",
      questions: [
        {
          id: "fruits",
          header: "Fruits",
          question: "Which fruits do you like?",
          options: [
            { label: "Apple", description: "Apple", value: "apple" },
            { label: "Banana", description: "Banana", value: "banana" },
            { label: "Cherry", description: "Cherry", value: "cherry" },
          ],
          allowCustomAnswer: true,
          required: false,
          multiSelect: true,
        },
      ],
    },
    {
      source: "ask-tool-form-shapes.jsonl:175",
      questions: [
        {
          id: "project_name",
          header: "Project name",
          question: "What should the project be called?",
          options: [],
          required: true,
        },
      ],
    },
    {
      source: "ext-ui-methods.jsonl:58",
      questions: [
        {
          id: "store",
          header: "Store",
          question: "Pick a store.\n\nSession store",
          options: [
            { label: "SQLite", description: "Default", value: "sqlite" },
            { label: "Redis", description: "Redis", value: "redis" },
          ],
          allowCustomAnswer: true,
          required: false,
        },
        { id: "notes", header: "Notes", question: "Notes", options: [], required: false },
      ],
    },
  ])("asks the recorded form $source one field at a time", ({ source, questions }) => {
    expect(ompFormQuestions(parsedForm(formHtml(source)))).toEqual(questions);
  });

  it("flattens context blocks into the prompt of the field they precede", () => {
    const form = parsedForm(`
      <p>Ignored: outside the form</p>
      <form>
        <h2>Deploy settings</h2>
        <p>Pick where &amp; how to deploy.</p>
        <table><tr><th>Env</th><th>Region</th></tr><tr><td>staging</td><td>eu-west</td></tr></table>
        <script>document.write("<input name='evil'>")</script>
        <!-- <input name="commented"> -->
        <label for="target">Target environment</label>
        <select id="target" name="target">
          <option value="dev" disabled>Dev</option>
          <option value="staging">Staging</option>
          <option value="prod" selected>Production</option>
        </select>
        <input type="hidden" name="token_hint" value="x">
        <ul><li>Rollbacks take 5&nbsp;min</li><li>Prod needs a ticket</li></ul>
        <textarea name="release_notes" aria-label="Release notes">
Fixes &lt;login&gt; bug</textarea>
        <input type="number" name="replicas" title="Replica count" value="2" min="1" max="5">
      </form>`);
    expect(form.title).toBe("Deploy settings");
    expect(form.fields.map((field) => field.name)).toEqual(["target", "release_notes", "replicas"]);
    expect(ompFormQuestions(form)).toEqual([
      {
        id: "target",
        header: "Target",
        question:
          "Deploy settings\n\nPick where & how to deploy.\n\nEnv | Region\nstaging | eu-west\n\nTarget environment",
        options: [
          { label: "Staging", description: "Staging", value: "staging" },
          { label: "Production", description: "Default", value: "prod" },
        ],
        allowCustomAnswer: false,
        required: false,
      },
      {
        id: "release_notes",
        header: "Release notes",
        question: "- Rollbacks take 5 min\n- Prod needs a ticket\n\nRelease notes",
        options: [],
        required: false,
        initialAnswer: "Fixes <login> bug",
      },
      {
        id: "replicas",
        header: "Replicas",
        question: "Replica count",
        options: [],
        required: false,
        initialAnswer: "2",
      },
    ]);
  });

  // Expected values are omp's own parse (pi-utils dom/parser.ts): omp rejects any other value.
  it.each([
    {
      name: "decodes only the entities omp decodes",
      html: "<form><select name=path><option>v1 &rarr; v2</option><option>Caf&eacute;</option><option>R &AMP; D</option><option>R &amp D</option><option>a&#38b</option><option>&#xD800;</option></select></form>",
      fields: [
        [
          "path",
          ["v1 &rarr; v2", "Café", "R & D", "R &amp D", "a&#38b", String.fromCodePoint(0xfffd)],
        ],
      ],
    },
    {
      name: "keeps a self-closed non-void tag open over what follows",
      html: '<form><input name="before"><button type="button"/><label>Age <input name="age"></label></form>',
      fields: [["before"]],
    },
    {
      name: "closes a self-closed tag inside svg",
      html: '<form><svg><g/></svg><input name="after"></form>',
      fields: [["after"]],
    },
    {
      name: "lets an unclosed quote run the tag on",
      html: '<form><label>Nick <input name="nick></label><input name="age"></form>',
      fields: [["nick></label><input name="]],
    },
    {
      name: "leaves an option open when an optgroup starts",
      html: '<form><select name="s"><option>A<optgroup label="g"><option>B</select></form>',
      fields: [["s", ["AB", "B"]]],
    },
    {
      name: "counts comment text in an option's text",
      html: '<form><select name="s"><option>A<!-- x -->B</option><option>C</option></select></form>',
      fields: [["s", ["A x B", "C"]]],
    },
    {
      name: "ignores a template's markup",
      html: '<template><form><input name="inert"></form></template><input name="live">',
      fields: [["live"]],
    },
  ])("$name, as omp does", ({ html, fields }) => {
    expect(
      parsedForm(html).fields.map((field) =>
        field.kind === "choice"
          ? [field.name, field.options.map((option) => option.value)]
          : [field.name],
      ),
    ).toEqual(fields);
  });

  it.each([
    { html: "<form><p>Nothing to answer</p><input type=submit></form>", reason: "no named fields" },
    {
      html: "<form><input type=radio name=x value=a><input type=text name=x></form>",
      reason: 'Two fields share the name "x"',
    },
    { html: '<form><input name=" padded "></form>', reason: "surrounding whitespace" },
  ])("refuses a form T3 cannot ask: $reason", ({ html, reason }) => {
    const result = parseOmpFormHtml(html);
    expect(result.ok).toBe(false);
    expect(result.ok ? "" : result.reason).toContain(reason);
  });

  it("reads text full of stray `<` in one pass over the form", () => {
    const html = `<form><p>${"< /".repeat(2_000)}</p><label>Name <input name="name"></label></form>`;
    // The parser runs on the server's event loop; a rescan per stray `<` grows with the square.
    const charAt = vi.spyOn(String.prototype, "charAt");
    const result = parseOmpFormHtml(html);
    const reads = charAt.mock.calls.length;
    charAt.mockRestore();

    expect(result.ok).toBe(true);
    expect(reads).toBeLessThan(html.length * 10);
  });
});

describe("ompFormData", () => {
  it.each([
    {
      source: "ask-tool-form-shapes.jsonl:37",
      answers: { colour: "red" },
      reply: "ask-tool-form-shapes.jsonl:39",
    },
    {
      source: "ask-tool-form-shapes.jsonl:85",
      answers: { fruits: ["apple", "banana", "kiwi (typed in Other)"] },
      reply: "ask-tool-form-shapes.jsonl:87",
    },
    {
      source: "ask-tool-form-shapes.jsonl:139",
      answers: { language: "python", editor: "vim" },
      reply: "ask-tool-form-shapes.jsonl:141",
    },
    {
      source: "ask-tool-form-shapes.jsonl:175",
      answers: { project_name: "Orbit" },
      reply: "ask-tool-form-shapes.jsonl:177",
    },
    {
      source: "ext-ui-methods.jsonl:58",
      answers: { store: "redis", notes: "keep it simple" },
      reply: "ext-ui-methods.jsonl:60",
    },
  ])("answers $source with the data omp accepted", ({ source, answers, reply }) => {
    const result = ompFormData(parsedForm(formHtml(source)), answers);
    expect(result).toEqual({ ok: true, data: fixtureFrame(reply)["data"] });
  });

  it.each([
    {
      name: "Other text stands in for a radio value; text fields always carry their text",
      source: "ext-ui-methods.jsonl:64",
      answers: { store: " memcached " },
      // omp's own result for the same answer (ext-ui-methods.jsonl:67).
      data: { store: "memcached", notes: "" },
    },
    {
      name: "a radio answer goes out as one string, never the array omp rejected (:73)",
      source: "ext-ui-methods.jsonl:70",
      answers: { store: ["redis"], notes: "" },
      data: { store: "redis", notes: "" },
    },
    {
      name: "answers to names the form lacks are never sent (omp failed the call at :118)",
      source: "ask-tool-form-cancel-timeout-invalid.jsonl:115",
      answers: { pet: "cats", not_a_field: "x" },
      data: { pet: "cats" },
    },
  ])("$name", ({ source, answers, data }) => {
    expect(ompFormData(parsedForm(formHtml(source)), answers)).toEqual({ ok: true, data });
  });

  it("sends numbers for number inputs and an extra checkbox entry for Other", () => {
    const form = parsedForm(`<form>
      <input type="number" name="count" min="1" max="5">
      <label><input type="checkbox" name="tags" value="a" checked> A</label>
      <label><input type="checkbox" name="tags" value="b"> B</label>
      <label><input type="checkbox" name="agree"> I agree</label>
    </form>`);
    expect(ompFormData(form, { count: "3", tags: ["b", "  c  "], agree: ["on"] })).toEqual({
      ok: true,
      data: { count: 3, tags: ["b", "c"], agree: "on" },
    });
    expect(ompFormData(form, { count: "", tags: [], agree: [] })).toEqual({
      ok: true,
      data: { count: "", tags: [] },
    });
  });

  it.each([
    { html: '<input name="city" required>', answers: { city: "  " }, reason: "Required" },
    {
      html: '<input type="number" name="n" min="1" max="5">',
      answers: { n: "9" },
      reason: "Must be at most 5",
    },
    { html: '<input type="number" name="n">', answers: { n: "many" }, reason: "Enter a number" },
    { html: '<input type="email" name="e">', answers: { e: "nobody" }, reason: "email" },
    { html: '<input type="url" name="u">', answers: { u: "example" }, reason: "full URL" },
    { html: '<input name="p" pattern="[a-z]+">', answers: { p: "ABC" }, reason: "Must match" },
    { html: '<input name="m" maxlength="3">', answers: { m: "abcd" }, reason: "At most 3" },
    {
      html: '<select name="s"><option>One</option><option>Two</option></select>',
      answers: { s: "Three" },
      reason: 'unexpected value "Three"',
    },
    {
      html: '<input type=radio name=r value=a><input type=radio name=r value=other aria-label="Other">',
      answers: { r: "typed" },
      reason: "unexpected value",
    },
    {
      html: "<input type=radio name=r value=a><input type=radio name=r value=b>",
      answers: { r: ["a", "b"] },
      reason: "takes one answer",
    },
    {
      html: "<input type=radio name=r value=a disabled><input type=radio name=r value=b>",
      answers: { r: "a" },
      reason: 'disabled option "a"',
    },
    {
      html: "<input type=checkbox name=c value=a required><input type=checkbox name=c value=b>",
      answers: { c: [] },
      reason: "Select at least one",
    },
  ])("fails the whole answer when one field is invalid: $reason", ({ html, answers, reason }) => {
    const form = parsedForm(`<form>${html}<input name="other_field"></form>`);
    const result = ompFormData(form, { other_field: "fine", ...answers });
    expect(result.ok).toBe(false);
    expect(result.ok ? "" : result.reason).toContain(reason);
  });
});
