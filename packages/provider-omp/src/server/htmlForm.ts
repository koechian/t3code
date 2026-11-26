/**
 * The `ask` tool's HTML form → T3 user-input questions, and T3 answers → the form's `{data}`.
 * Ports the subset of oh-my-pi tools/html-form/{parse,data}.ts that T3 can render, plus the
 * answer checks omp's RPC host applies (modes/rpc/rpc-mode.ts `parseFormDialogResponse`), with
 * no DOM dependency. Pure; imports nothing from this package.
 */
import type {
  OrchestrationV2UserInputQuestion,
  ProviderUserInputAnswers,
} from "@t3tools/contracts";

export interface OmpFormOption {
  readonly value: string;
  readonly label: string;
  readonly checked: boolean;
  /** omp rejects an answer that picks a disabled option. */
  readonly disabled: boolean;
}

export type OmpFormField =
  | {
      readonly kind: "choice";
      readonly name: string;
      /** checkbox group or `<select multiple>` */
      readonly multi: boolean;
      /** `<select>`: never offers Other */
      readonly select: boolean;
      /** radio/checkbox group of 2+ options without its own "Other" */
      readonly other: boolean;
      readonly label: string;
      readonly options: ReadonlyArray<OmpFormOption>;
      readonly required: boolean;
      /** Its label, else the text right before it, its section title, a lone option's label or its name. */
      readonly question: string;
      /** Form text its prompt carries when the form is asked one field at a time; "" when none. */
      readonly context: string;
    }
  | {
      readonly kind: "text";
      readonly name: string;
      /** `text` | `textarea` | `number` | `email` | `url` | … */
      readonly type: string;
      readonly label: string;
      readonly value: string;
      readonly placeholder: string;
      readonly required: boolean;
      readonly min: number | undefined;
      readonly max: number | undefined;
      readonly minLength: number | undefined;
      readonly maxLength: number | undefined;
      readonly pattern: string | undefined;
      readonly question: string;
      readonly context: string;
    };

type OmpChoiceFormField = Extract<OmpFormField, { readonly kind: "choice" }>;
type OmpTextFormField = Extract<OmpFormField, { readonly kind: "text" }>;

export interface OmpParsedForm {
  /** The first heading (`h1`-`h6`, `summary`), if any. */
  readonly title: string | undefined;
  /** Context blocks (paragraphs, lists, tables, pre) flattened to text, in document order. */
  readonly context: string;
  /** Named fields only: omp never reads an unnamed control. */
  readonly fields: ReadonlyArray<OmpFormField>;
}

export type OmpFormParseResult =
  | { readonly ok: true; readonly form: OmpParsedForm }
  | { readonly ok: false; readonly reason: string };

export type OmpFormDataResult =
  | {
      readonly ok: true;
      readonly data: Readonly<Record<string, string | number | ReadonlyArray<string>>>;
    }
  | { readonly ok: false; readonly reason: string };

type OmpFormValue = string | number | ReadonlyArray<string>;

export function parseOmpFormHtml(html: string): OmpFormParseResult {
  const document = parseHtmlDocument(html);
  const root = findFirst(document, FORM_TAGS) ?? document;
  const { sections, fields } = new OmpFormBuilder(root).build();
  const named = fields.filter((field) => field.name !== "");
  const problem = namedFieldsProblem(named);
  if (problem !== undefined) return { ok: false, reason: problem };
  const questions = fieldQuestions(sections);
  const contexts = fieldContexts(sections, questions);
  return {
    ok: true,
    form: {
      title: formTitle(sections),
      context: formContext(sections, questions),
      fields: named.map((field) => ({
        ...field,
        question: questions.get(field) ?? field.name,
        context: contexts.get(field) ?? "",
      })),
    },
  };
}

/** One question per field; question ids are the field names. Synthesizes header/description. */
export function ompFormQuestions(
  form: OmpParsedForm,
): ReadonlyArray<OrchestrationV2UserInputQuestion> {
  return form.fields.flatMap((field) => {
    const question = field.kind === "choice" ? choiceQuestion(field) : textQuestion(field);
    return question === undefined ? [] : [question];
  });
}

/**
 * Option values (not labels), numbers for `number`, Other text as the radio value or one extra
 * checkbox entry. Validates every field first: one bad field fails the whole ask call in omp.
 */
export function ompFormData(
  form: OmpParsedForm,
  answers: ProviderUserInputAnswers,
): OmpFormDataResult {
  const entries: Array<[string, OmpFormValue]> = [];
  for (const field of form.fields) {
    const answer = Object.hasOwn(answers, field.name) ? answers[field.name] : undefined;
    const result = fieldData(field, answer);
    if (!result.ok) {
      return { ok: false, reason: `Form field ${JSON.stringify(field.name)}: ${result.reason}` };
    }
    if (result.value !== undefined) entries.push([field.name, result.value]);
  }
  // Entries, not assignment, so a field named `__proto__` stays an own key.
  return { ok: true, data: Object.fromEntries(entries) };
}

const DEFAULT_OPTION_DESCRIPTION = "Default";

function questionBase(field: OmpFormField) {
  return {
    id: field.name,
    header: fieldHeader(field.name),
    question: field.context === "" ? field.question : `${field.context}\n\n${field.question}`,
  };
}

function textQuestion(field: OmpTextFormField): OrchestrationV2UserInputQuestion {
  return {
    ...questionBase(field),
    options: [],
    required: field.required,
    ...(field.value === "" ? {} : { initialAnswer: field.value }),
  };
}

function choiceQuestion(field: OmpChoiceFormField): OrchestrationV2UserInputQuestion | undefined {
  const options = offeredOptions(field);
  if (options.length === 0 && !field.other) return undefined;
  return {
    ...questionBase(field),
    options: options.map(questionOption),
    allowCustomAnswer: field.other,
    required: field.required,
    ...(field.multi ? { multiSelect: true } : {}),
  };
}

/** Enabled options, first of each value: answers carry values, so duplicates are one choice. */
function offeredOptions(field: OmpChoiceFormField): ReadonlyArray<OmpFormOption> {
  const seen = new Set<string>();
  return field.options.filter((option) => {
    if (option.disabled || seen.has(option.value)) return false;
    seen.add(option.value);
    return true;
  });
}

function questionOption(option: OmpFormOption, index: number) {
  const label = option.label.trim() || option.value.trim() || `Option ${index + 1}`;
  return {
    label,
    description: option.checked ? DEFAULT_OPTION_DESCRIPTION : label,
    value: option.value,
  };
}

/** `project_name` → `Project name`, `projectName` → `Project Name`. */
function fieldHeader(name: string): string {
  const words = name
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/[\s_.-]+/g, " ")
    .trim();
  return words === "" ? "Question" : words.charAt(0).toUpperCase() + words.slice(1);
}

type FieldDataResult =
  | { readonly ok: true; readonly value: OmpFormValue | undefined }
  | { readonly ok: false; readonly reason: string };

function fieldData(field: OmpFormField, answer: unknown): FieldDataResult {
  const parsed = field.kind === "choice" ? choiceData(field, answer) : textData(field, answer);
  if (!parsed.ok) return parsed;
  const problem =
    field.kind === "choice" ? choiceError(field, parsed.value) : textError(field, parsed.value);
  return problem === undefined ? parsed : { ok: false, reason: problem };
}

function choiceData(field: OmpChoiceFormField, answer: unknown): FieldDataResult {
  const entries = answerEntries(answer);
  if (entries === undefined) return { ok: false, reason: "must carry text or a list of options" };
  const checked = new Set<string>();
  let other: string | undefined;
  for (const entry of entries) {
    const outcome = classifyChoiceEntry(field, entry, other);
    if (outcome.kind === "invalid") return { ok: false, reason: outcome.reason };
    if (outcome.kind === "option") checked.add(entry);
    if (outcome.kind === "other") other = outcome.text;
  }
  if (!isArrayChoice(field) && checked.size + (other === undefined ? 0 : 1) > 1) {
    return { ok: false, reason: "takes one answer" };
  }
  return { ok: true, value: choiceValue(field, checked, other) };
}

type ChoiceEntry =
  | { readonly kind: "option" }
  | { readonly kind: "other"; readonly text: string }
  | { readonly kind: "blank" }
  | { readonly kind: "invalid"; readonly reason: string };

function classifyChoiceEntry(
  field: OmpChoiceFormField,
  entry: string,
  other: string | undefined,
): ChoiceEntry {
  if (field.options.some((option) => option.value === entry && !option.disabled)) {
    return { kind: "option" };
  }
  if (field.options.some((option) => option.value === entry)) {
    return { kind: "invalid", reason: `carries disabled option ${JSON.stringify(entry)}` };
  }
  const text = entry.trim();
  if (text === "") return { kind: "blank" };
  if (!field.other || other !== undefined) {
    return { kind: "invalid", reason: `carries unexpected value ${JSON.stringify(entry)}` };
  }
  return { kind: "other", text };
}

/** upstream `choiceValue`: the Other text stands in for a radio's value or joins a checkbox array. */
function choiceValue(
  field: OmpChoiceFormField,
  checked: ReadonlySet<string>,
  other: string | undefined,
): OmpFormValue | undefined {
  const values = [
    ...new Set(
      field.options
        .filter((option) => !option.disabled && checked.has(option.value))
        .map((option) => option.value),
    ),
  ];
  if (isArrayChoice(field)) return other === undefined ? values : [...values, other];
  return other ?? values[0];
}

/** omp takes an array only for a checkbox group or multi-select of 2+ options. */
function isArrayChoice(field: OmpChoiceFormField): boolean {
  return field.multi && field.options.length > 1;
}

function textData(field: OmpTextFormField, answer: unknown): FieldDataResult {
  if (answer === undefined || answer === null) return { ok: true, value: textValue(field, "") };
  if (typeof answer === "number" && isNumericField(field) && Number.isFinite(answer)) {
    return { ok: true, value: answer };
  }
  const text = singleText(answer);
  return text === undefined
    ? { ok: false, reason: "must carry text" }
    : { ok: true, value: textValue(field, text) };
}

function textValue(field: OmpTextFormField, text: string): OmpFormValue {
  if (!isNumericField(field) || text.trim() === "") return text;
  const number = Number(text);
  return Number.isFinite(number) ? number : text;
}

function isNumericField(field: OmpTextFormField): boolean {
  return field.type === "number" || field.type === "range";
}

function choiceError(field: OmpChoiceFormField, value: OmpFormValue | undefined) {
  const empty = value === undefined || (Array.isArray(value) && value.length === 0);
  if (!field.required || !empty) return undefined;
  return isArrayChoice(field) ? "Select at least one" : "Required";
}

/** upstream `fieldError` for text: required, number range, length, email, url and pattern. */
function textError(field: OmpTextFormField, value: OmpFormValue | undefined) {
  const text = value === undefined ? "" : String(value);
  if (text.trim() === "") return field.required ? "Required" : undefined;
  return numberError(field, text) ?? lengthError(field, text) ?? shapeError(field, text);
}

function numberError(field: OmpTextFormField, text: string): string | undefined {
  if (!isNumericField(field)) return undefined;
  const number = Number(text);
  if (!Number.isFinite(number)) return "Enter a number";
  if (field.min !== undefined && number < field.min) return `Must be at least ${field.min}`;
  if (field.max !== undefined && number > field.max) return `Must be at most ${field.max}`;
  return undefined;
}

function lengthError(field: OmpTextFormField, text: string): string | undefined {
  if (field.minLength !== undefined && text.length < field.minLength) {
    return `Needs at least ${field.minLength} characters`;
  }
  if (field.maxLength !== undefined && text.length > field.maxLength) {
    return `At most ${field.maxLength} characters`;
  }
  return undefined;
}

function shapeError(field: OmpTextFormField, text: string): string | undefined {
  if (field.type === "email" && !/^[^\s@]+@[^\s@]+$/.test(text)) return "Enter an email address";
  if (field.type === "url" && !URL.canParse(text)) return "Enter a full URL";
  if (field.pattern !== undefined && compilePattern(field.pattern)?.test(text) === false) {
    const shown = field.pattern.length > 40 ? `${field.pattern.slice(0, 39)}…` : field.pattern;
    return `Must match ${shown}`;
  }
  return undefined;
}

/** `^(?:pattern)$` as HTML matches `pattern`; undefined when the model wrote an invalid one. */
function compilePattern(pattern: string): RegExp | undefined {
  try {
    return new RegExp(`^(?:${pattern})$`, "v");
  } catch {
    return undefined;
  }
}

function answerEntries(answer: unknown): ReadonlyArray<string> | undefined {
  if (answer === undefined || answer === null) return [];
  if (typeof answer === "string") return [answer];
  if (!Array.isArray(answer)) return undefined;
  return answer.every((entry) => typeof entry === "string") ? answer : undefined;
}

function singleText(answer: unknown): string | undefined {
  if (typeof answer === "string") return answer;
  return Array.isArray(answer) && answer.length === 1 && typeof answer[0] === "string"
    ? answer[0]
    : undefined;
}

interface ChoiceDraft {
  readonly kind: "choice";
  readonly name: string;
  readonly multi: boolean;
  readonly select: boolean;
  other: boolean;
  readonly label: string;
  readonly options: Array<OmpFormOption>;
  required: boolean;
}

interface TextDraft {
  readonly kind: "text";
  readonly name: string;
  readonly type: string;
  readonly label: string;
  readonly value: string;
  readonly placeholder: string;
  readonly required: boolean;
  readonly min: number | undefined;
  readonly max: number | undefined;
  readonly minLength: number | undefined;
  readonly maxLength: number | undefined;
  readonly pattern: string | undefined;
}

type FieldDraft = ChoiceDraft | TextDraft;

/** A radio/checkbox group whose options are split by other blocks ends just past the last one. */
interface FieldBlock {
  readonly kind: "field";
  readonly field: FieldDraft;
  optionsEnd?: number;
}

type FormBlock =
  | { readonly kind: "text"; readonly text: string; readonly heading: boolean }
  | { readonly kind: "list"; readonly items: ReadonlyArray<string>; readonly ordered: boolean }
  | { readonly kind: "pre"; readonly text: string }
  | { readonly kind: "table"; readonly rows: ReadonlyArray<ReadonlyArray<string>> }
  | { readonly kind: "media" }
  | FieldBlock;

interface FormSection {
  title: string;
  readonly blocks: Array<FormBlock>;
}

interface ControlBase {
  readonly name: string;
  readonly required: boolean;
  readonly label: string;
}

/** A comment's pseudo-tag: its text counts in `textContent` but is never walked or labelled. */
const COMMENT_TAG = "#comment";
const SKIPPED_TAGS = new Set([
  COMMENT_TAG,
  "button",
  "head",
  "iframe",
  "link",
  "meta",
  "noscript",
  "script",
  "style",
  "template",
  "title",
]);
const BLOCK_TAGS = new Set([
  "address",
  "article",
  "aside",
  "blockquote",
  "dd",
  "details",
  "div",
  "dl",
  "dt",
  "figure",
  "figcaption",
  "footer",
  "header",
  "li",
  "main",
  "nav",
  "p",
  "section",
]);
const HEADING_TAGS = new Set(["h1", "h2", "h3", "h4", "h5", "h6", "summary"]);
const CONTROL_TAGS = new Set(["input", "select", "textarea"]);
const INLINE_EXCLUDED_TAGS = new Set([...SKIPPED_TAGS, ...CONTROL_TAGS, "svg"]);
const IGNORED_INPUT_TYPES = new Set(["button", "file", "image", "reset", "submit", "hidden"]);
const FORM_TAGS = new Set(["form"]);
const LABEL_TAGS = new Set(["label"]);
const OPTION_TAGS = new Set(["option"]);
const ROW_TAGS = new Set(["tr"]);
const OTHER_OPTION_LABEL = /^\s*other\b/i;

/** upstream parse.ts `FormBuilder`, keeping plain text instead of styled spans. */
class OmpFormBuilder {
  readonly #root: HtmlElement;
  readonly #sections: Array<FormSection> = [];
  readonly #fields: Array<FieldDraft> = [];
  /** Radio/checkbox groups by `type\0name`, with the section and block of their first option. */
  readonly #groups = new Map<
    string,
    { readonly field: ChoiceDraft; readonly section: FormSection; readonly block: FieldBlock }
  >();
  /** `<label for>` elements by the id of the control they name. */
  readonly #labelFor = new Map<string, HtmlElement>();
  #section: FormSection = { title: "", blocks: [] };
  #inline = "";
  #fieldsetDepth = 0;
  readonly #handlers = new Map<string, (element: HtmlElement) => void>([
    ["br", () => this.#lineBreak()],
    ["hr", () => this.#flush(false)],
    ["input", (element) => this.#control(element, "")],
    ["select", (element) => this.#control(element, "")],
    ["textarea", (element) => this.#control(element, "")],
    ["label", (element) => this.#label(element)],
    ["fieldset", (element) => this.#fieldset(element)],
    ["legend", (element) => this.#block(element, true)],
    ["ul", (element) => this.#list(element, false)],
    ["ol", (element) => this.#list(element, true)],
    ["pre", (element) => this.#pre(element)],
    ["table", (element) => this.#table(element)],
    ["img", (element) => this.#image(element)],
    ["svg", () => this.#media()],
  ]);

  constructor(root: HtmlElement) {
    this.#root = root;
    this.#sections.push(this.#section);
  }

  build() {
    this.#indexLabels();
    this.#children(this.#root);
    this.#flush(false);
    for (const field of this.#fields) markOther(field);
    const sections: ReadonlyArray<FormSection> = this.#sections.filter(
      (section) => section.blocks.length > 0 || section.title !== "",
    );
    const fields: ReadonlyArray<FieldDraft> = this.#fields;
    return { sections, fields };
  }

  #indexLabels(): void {
    const ids = new Set(
      findAll(this.#root, CONTROL_TAGS).flatMap((control) => {
        const id = attribute(control, "id");
        return id ? [id] : [];
      }),
    );
    for (const label of findAll(this.#root, LABEL_TAGS)) {
      const target = attribute(label, "for");
      if (target && ids.has(target) && !this.#labelFor.has(target)) {
        this.#labelFor.set(target, label);
      }
    }
  }

  #children(element: HtmlElement): void {
    for (const child of element.children) this.#walk(child);
  }

  #walk(node: HtmlNode): void {
    if (typeof node === "string") {
      this.#inline += node;
      return;
    }
    if (SKIPPED_TAGS.has(node.tag)) return;
    const handler = this.#handlers.get(node.tag);
    if (handler !== undefined) return handler(node);
    if (HEADING_TAGS.has(node.tag)) return this.#block(node, true);
    if (BLOCK_TAGS.has(node.tag)) return this.#block(node, false);
    this.#children(node);
  }

  #lineBreak(): void {
    this.#inline += "\n";
  }

  #block(element: HtmlElement, heading: boolean): void {
    this.#flush(false);
    this.#children(element);
    this.#flush(heading);
  }

  /** Emit buffered inline content as a text block. */
  #flush(heading: boolean): void {
    const text = normalizeText(this.#inline);
    this.#inline = "";
    if (text !== "") this.#section.blocks.push({ kind: "text", text, heading });
  }

  #label(label: HtmlElement): void {
    const target = attribute(label, "for");
    if (target !== undefined && this.#labelFor.get(target) === label) return;
    const controls = findAll(label, CONTROL_TAGS);
    if (controls.length === 0) return this.#children(label);
    this.#flush(false);
    const text = inlineText(label);
    for (const control of controls) this.#control(control, text);
  }

  #fieldset(fieldset: HtmlElement): void {
    this.#flush(false);
    const legend = childElements(fieldset).find((child) => child.tag === "legend");
    // Nested fieldsets stay in the outer section; their legend becomes a heading.
    if (this.#fieldsetDepth > 0) {
      if (legend !== undefined) this.#walk(legend);
    } else {
      this.#startSection(legend === undefined ? "" : inlineText(legend));
    }
    this.#fieldsetDepth++;
    for (const child of fieldset.children) if (child !== legend) this.#walk(child);
    this.#flush(false);
    this.#fieldsetDepth--;
    if (this.#fieldsetDepth === 0) this.#startSection("");
  }

  #startSection(title: string): void {
    if (this.#section.blocks.length === 0 && this.#section.title === "") {
      this.#section.title = title;
      return;
    }
    this.#section = { title, blocks: [] };
    this.#sections.push(this.#section);
  }

  #list(list: HtmlElement, ordered: boolean): void {
    this.#flush(false);
    // A list of controls is layout, not content: walk it as flow.
    if (findFirst(list, CONTROL_TAGS) !== undefined) {
      this.#children(list);
      this.#flush(false);
      return;
    }
    const items = childElements(list)
      .filter((child) => child.tag === "li")
      .map(inlineText)
      .filter((text) => text !== "");
    if (items.length > 0) this.#section.blocks.push({ kind: "list", items, ordered });
  }

  #pre(pre: HtmlElement): void {
    this.#flush(false);
    const text = textContent(pre).replace(/^\n/, "").trimEnd();
    this.#section.blocks.push({ kind: "pre", text });
  }

  #image(image: HtmlElement): void {
    if (attribute(image, "src")?.trim()) this.#media();
  }

  #media(): void {
    this.#flush(false);
    this.#section.blocks.push({ kind: "media" });
  }

  #table(table: HtmlElement): void {
    this.#flush(false);
    const rows = findAll(table, ROW_TAGS).filter((row) => closest(row, "table") === table);
    if (findFirst(table, CONTROL_TAGS) === undefined) {
      const cells = rows.map((row) => tableCells(row).map(inlineText));
      if (cells.some((row) => row.length > 0)) {
        this.#section.blocks.push({ kind: "table", rows: cells });
      }
      return;
    }
    // A table of controls (a decision matrix): each row's text becomes a line and each
    // unlabeled control borrows its column header as its label.
    let header: ReadonlyArray<string> = [];
    for (const row of rows) {
      const cells = tableCells(row);
      if (isHeaderRow(row, cells)) header = cells.map(inlineText);
      else this.#controlRow(cells, header);
    }
  }

  #controlRow(cells: ReadonlyArray<HtmlElement>, header: ReadonlyArray<string>): void {
    const text = cells
      .filter((cell) => findFirst(cell, CONTROL_TAGS) === undefined)
      .map(inlineText)
      .filter((cellText) => cellText !== "")
      .join(" · ");
    if (text !== "") this.#section.blocks.push({ kind: "text", text, heading: false });
    cells.forEach((cell, column) => {
      for (const control of findAll(cell, CONTROL_TAGS)) {
        this.#control(control, controlCellLabel(control, cell) || (header[column] ?? ""));
      }
    });
  }

  #control(control: HtmlElement, wrapping: string): void {
    this.#flush(false);
    const base: ControlBase = {
      name: attribute(control, "name") ?? "",
      required: hasAttribute(control, "required"),
      label: wrapping !== "" ? wrapping : this.#detachedLabel(control),
    };
    if (control.tag === "select") {
      this.#addField(selectField(control, base));
    } else if (control.tag === "textarea") {
      this.#addField(textareaField(control, base));
    } else {
      this.#input(control, base);
    }
  }

  #input(control: HtmlElement, base: ControlBase): void {
    const type = (attribute(control, "type") ?? "text").toLowerCase();
    if (IGNORED_INPUT_TYPES.has(type)) return;
    if (type === "radio" || type === "checkbox") return this.#choiceOption(control, type, base);
    this.#addField(inputField(control, type, base));
  }

  #choiceOption(control: HtmlElement, type: "radio" | "checkbox", base: ControlBase): void {
    const value = attribute(control, "value");
    const option: OmpFormOption = {
      value: value ?? "on",
      label: base.label !== "" ? base.label : (value ?? base.name),
      checked: hasAttribute(control, "checked"),
      disabled: hasAttribute(control, "disabled"),
    };
    const key = `${type}\0${base.name}`;
    const group = this.#groups.get(key);
    if (group === undefined) {
      const field: ChoiceDraft = {
        kind: "choice",
        name: base.name,
        multi: type === "checkbox",
        select: false,
        other: false,
        label: "",
        options: [option],
        required: base.required,
      };
      this.#groups.set(key, { field, section: this.#section, block: this.#addField(field) });
      return;
    }
    group.field.options.push(option);
    group.field.required ||= base.required;
    if (group.section === this.#section && this.#section.blocks.at(-1) !== group.block) {
      group.block.optionsEnd = this.#section.blocks.length;
    }
  }

  /** Label of a control outside any wrapping `<label>`: `<label for>`, then `aria-label`, `title`. */
  #detachedLabel(control: HtmlElement): string {
    const id = attribute(control, "id");
    const label = id ? this.#labelFor.get(id) : undefined;
    if (label !== undefined) return inlineText(label);
    return normalizeText(attribute(control, "aria-label") ?? attribute(control, "title") ?? "");
  }

  #addField(field: FieldDraft): FieldBlock {
    const block: FieldBlock = { kind: "field", field };
    this.#fields.push(field);
    this.#section.blocks.push(block);
    return block;
  }
}

function markOther(field: FieldDraft): void {
  if (field.kind !== "choice" || field.select || field.options.length < 2) return;
  field.other = !field.options.some((option) => OTHER_OPTION_LABEL.test(option.label));
}

function selectField(control: HtmlElement, base: ControlBase): ChoiceDraft {
  const multi = hasAttribute(control, "multiple");
  const options = findAll(control, OPTION_TAGS).map(selectOption);
  return {
    kind: "choice",
    name: base.name,
    multi,
    select: true,
    other: false,
    label: base.label,
    options: multi ? [...options] : withSingleSelection(options),
    required: base.required,
  };
}

function selectOption(option: HtmlElement): OmpFormOption {
  const text = oneLine(textContent(option));
  return {
    value: attribute(option, "value") ?? text,
    label: text,
    checked: hasAttribute(option, "selected"),
    disabled: hasAttribute(option, "disabled"),
  };
}

/** A single select always has a selection: the first enabled option unless one is marked. */
function withSingleSelection(options: ReadonlyArray<OmpFormOption>): Array<OmpFormOption> {
  if (options.some((option) => option.checked)) return [...options];
  const first = options.findIndex((option) => !option.disabled);
  return options.map((option, index) => (index === first ? { ...option, checked: true } : option));
}

function textareaField(control: HtmlElement, base: ControlBase): TextDraft {
  return {
    kind: "text",
    name: base.name,
    type: "textarea",
    label: base.label !== "" ? base.label : nameLabel(base.name),
    value: textContent(control).replace(/^\r?\n/, ""),
    placeholder: attribute(control, "placeholder") ?? "",
    required: base.required,
    min: undefined,
    max: undefined,
    minLength: numberAttribute(control, "minlength"),
    maxLength: numberAttribute(control, "maxlength"),
    pattern: undefined,
  };
}

function inputField(control: HtmlElement, type: string, base: ControlBase): TextDraft {
  return {
    kind: "text",
    name: base.name,
    type,
    label: base.label !== "" ? base.label : nameLabel(base.name),
    value: attribute(control, "value") ?? "",
    placeholder: attribute(control, "placeholder") ?? "",
    required: base.required,
    min: numberAttribute(control, "min"),
    max: numberAttribute(control, "max"),
    minLength: numberAttribute(control, "minlength"),
    maxLength: numberAttribute(control, "maxlength"),
    pattern: attribute(control, "pattern"),
  };
}

/** Last-resort label from the control name: `sentry_dsn` → `sentry dsn`. */
function nameLabel(name: string): string {
  return name.replace(/[_-]+/g, " ");
}

function numberAttribute(element: HtmlElement, name: string): number | undefined {
  const raw = attribute(element, name);
  if (raw === undefined || raw.trim() === "") return undefined;
  const value = Number(raw);
  return Number.isFinite(value) ? value : undefined;
}

function tableCells(row: HtmlElement): ReadonlyArray<HtmlElement> {
  return childElements(row).filter((cell) => cell.tag === "td" || cell.tag === "th");
}

function isHeaderRow(row: HtmlElement, cells: ReadonlyArray<HtmlElement>): boolean {
  return cells.every((cell) => cell.tag === "th") && findFirst(row, CONTROL_TAGS) === undefined;
}

function controlCellLabel(control: HtmlElement, cell: HtmlElement): string {
  const own = closest(control, "label");
  return own !== undefined && contains(cell, own) ? inlineText(own) : inlineText(cell);
}

function namedFieldsProblem(fields: ReadonlyArray<FieldDraft>): string | undefined {
  if (fields.length === 0) return "The form has no named fields";
  const seen = new Set<string>();
  for (const field of fields) {
    if (field.name.trim() !== field.name) {
      return `Field name ${JSON.stringify(field.name)} has surrounding whitespace`;
    }
    if (seen.has(field.name)) return `Two fields share the name ${JSON.stringify(field.name)}`;
    seen.add(field.name);
  }
  return undefined;
}

function formTitle(sections: ReadonlyArray<FormSection>): string | undefined {
  for (const section of sections) {
    for (const block of section.blocks) {
      if (block.kind === "text" && block.heading) return block.text;
    }
  }
  return undefined;
}

/**
 * upstream `fieldQuestions`: its own label, else the text block right before it, else its section
 * title, else a lone option's label (a single checkbox), else its name.
 */
function fieldQuestions(sections: ReadonlyArray<FormSection>): Map<FieldDraft, string> {
  const questions = new Map<FieldDraft, string>();
  for (const section of sections) {
    let caption = "";
    for (const block of section.blocks) {
      if (block.kind !== "field") {
        caption = block.kind === "text" ? block.text : "";
        continue;
      }
      questions.set(block.field, fieldQuestion(block.field, caption, section.title));
      caption = "";
    }
  }
  return questions;
}

function fieldQuestion(field: FieldDraft, caption: string, title: string): string {
  const lone =
    field.kind === "choice" && field.options.length === 1 ? (field.options[0]?.label ?? "") : "";
  return oneLine(field.label || caption || title || lone || field.name);
}

interface ContextState {
  pending: Array<string>;
  last: Array<string> | undefined;
  readonly parts: Map<FieldDraft, Array<string>>;
}

interface ContextOwner {
  readonly parts: Array<string>;
  readonly end: number;
}

/**
 * upstream `fieldContexts`: section titles and context blocks since the previous named field, text
 * between the field's own options, and, for the last field, the text after it. Text that already
 * is the field's question is left out.
 */
function fieldContexts(
  sections: ReadonlyArray<FormSection>,
  questions: ReadonlyMap<FieldDraft, string>,
): Map<FieldDraft, string> {
  const state: ContextState = { pending: [], last: undefined, parts: new Map() };
  for (const section of sections) collectSectionContexts(section, questions, state);
  state.last?.push(...state.pending);
  return new Map([...state.parts].map(([field, parts]) => [field, parts.join("\n\n")]));
}

function collectSectionContexts(
  section: FormSection,
  questions: ReadonlyMap<FieldDraft, string>,
  state: ContextState,
): void {
  if (section.title !== "") state.pending.push(section.title);
  let owner: ContextOwner | undefined;
  section.blocks.forEach((block, index) => {
    if (block.kind === "field") {
      owner = claimContext(section, index, block, questions, state) ?? owner;
      return;
    }
    const text = blockText(block);
    if (text === undefined || text.trim() === "") return;
    if (owner !== undefined && index < owner.end) owner.parts.push(text);
    else state.pending.push(text);
  });
}

function claimContext(
  section: FormSection,
  index: number,
  block: FieldBlock,
  questions: ReadonlyMap<FieldDraft, string>,
  state: ContextState,
): ContextOwner | undefined {
  if (block.field.name === "") return undefined;
  const question = questions.get(block.field);
  const own = state.pending.filter((part) => oneLine(part) !== question);
  state.parts.set(block.field, own);
  state.pending = [];
  state.last = own;
  const end = block.optionsEnd ?? 0;
  // A named field among the group's options keeps the text between them for itself.
  const intervening = section.blocks.some(
    (later, at) => at > index && at < end && later.kind === "field" && later.field.name !== "",
  );
  return { parts: own, end: intervening ? 0 : end };
}

/** The section titles and context blocks, leaving out text that already is a field's question. */
function formContext(
  sections: ReadonlyArray<FormSection>,
  questions: ReadonlyMap<FieldDraft, string>,
): string {
  const asked = new Set(
    [...questions].flatMap(([field, question]) => (field.name === "" ? [] : [question])),
  );
  return sections
    .flatMap((section) => [section.title, ...section.blocks.map(blockText)])
    .filter(
      (text): text is string =>
        text !== undefined && text.trim() !== "" && !asked.has(oneLine(text)),
    )
    .join("\n\n");
}

function blockText(block: FormBlock): string | undefined {
  switch (block.kind) {
    case "text":
    case "pre":
      return block.text;
    case "list":
      return block.items
        .map((item, index) => `${block.ordered ? `${index + 1}.` : "-"} ${item}`)
        .join("\n");
    case "table":
      return block.rows.map((cells) => cells.join(" | ")).join("\n");
    default:
      return undefined;
  }
}

/** Collapse whitespace like HTML layout: runs become one space, spaces next to a break vanish. */
function normalizeText(text: string): string {
  return text
    .replace(/[^\S\n]+/g, " ")
    .replace(/ ?\n ?/g, "\n")
    .trim();
}

function oneLine(text: string): string {
  return text.replace(/\s+/g, " ").trim();
}

/** Inline text of `element`'s children, skipping controls (their label is the text around them). */
function inlineText(element: HtmlElement): string {
  return normalizeText(element.children.map(inlineNodeText).join(""));
}

function inlineNodeText(node: HtmlNode): string {
  if (typeof node === "string") return node;
  if (INLINE_EXCLUDED_TAGS.has(node.tag)) return "";
  if (node.tag === "br") return "\n";
  // Labels stay text: a picture inside one reads as its alt text.
  if (node.tag === "img") return attribute(node, "alt") ?? "";
  const inner = node.children.map(inlineNodeText).join("");
  return BLOCK_TAGS.has(node.tag) || HEADING_TAGS.has(node.tag) ? ` ${inner}` : inner;
}

/**
 * A port of omp's HTML parser (pi-utils `dom/parser.ts`), quirks included: omp answers the form
 * from its own parse, so a field or option value it reads differently gets the reply rejected.
 */
interface HtmlElement {
  readonly tag: string;
  /** In the SVG namespace, where `<x/>` closes and `textarea`/`title` hold markup. */
  readonly svg: boolean;
  readonly attributes: ReadonlyMap<string, string>;
  readonly children: Array<HtmlNode>;
  readonly parent: HtmlElement | undefined;
}

type HtmlNode = HtmlElement | string;

interface StartTag {
  readonly name: string;
  readonly attributes: ReadonlyMap<string, string>;
  readonly selfClosing: boolean;
}

interface RawText {
  readonly end: RegExp;
  readonly decode: boolean;
}

const VOID_TAGS = new Set([
  "area",
  "base",
  "br",
  "col",
  "embed",
  "hr",
  "img",
  "input",
  "link",
  "meta",
  "param",
  "source",
  "track",
  "wbr",
]);
const RAW_TEXT = new Map<string, RawText>([
  ["script", { end: /<\/script\s*>/gi, decode: false }],
  ["style", { end: /<\/style\s*>/gi, decode: false }],
]);
const RCDATA = new Map<string, RawText>([
  ["textarea", { end: /<\/textarea\s*>/gi, decode: true }],
  ["title", { end: /<\/title\s*>/gi, decode: true }],
]);
/** Only the innermost open element is ever closed by a start tag. */
const P_CLOSERS = new Set([
  "address",
  "article",
  "aside",
  "blockquote",
  "div",
  "dl",
  "fieldset",
  "footer",
  "form",
  "h1",
  "h2",
  "h3",
  "h4",
  "h5",
  "h6",
  "header",
  "hr",
  "menu",
  "nav",
  "ol",
  "p",
  "pre",
  "section",
  "table",
  "ul",
]);
const DEFINITION_TAGS = new Set(["dt", "dd"]);
const CELL_TAGS = new Set(["th", "td"]);
const TABLE_SECTION_TAGS = new Set(["thead", "tbody", "tfoot"]);
const CLOSED_BY = new Map<string, ReadonlySet<string>>([
  ["li", new Set(["li"])],
  ["dt", DEFINITION_TAGS],
  ["dd", DEFINITION_TAGS],
  ["tr", new Set(["tr"])],
  ["th", CELL_TAGS],
  ["td", CELL_TAGS],
  ["option", new Set(["option"])],
  ["thead", TABLE_SECTION_TAGS],
  ["tbody", TABLE_SECTION_TAGS],
  ["tfoot", TABLE_SECTION_TAGS],
]);
/** omp parses an RSS or Atom document by XML rules: only `<x/>` closes, and no tag holds raw text. */
const XML_FEED = /^\s*(?:<\?xml[\s\S]*?\?>\s*)?<(?:feed|rss)\b/i;
const SPACE = /\s/;
const TAG_NAME = /[^\s/>]+/y;
const ATTRIBUTE_NAME = /[^\s=/>]+/y;
const UNQUOTED_VALUE = /[^\s>]+/y;
const ENTITY = /&(#(?:x[0-9a-f]+|\d+)|[a-z][a-z0-9]+);/gi;
const REPLACEMENT_CHARACTER = String.fromCodePoint(0xfffd);
/** omp's named entities as code points; any other name stays as written. */
const NAMED_ENTITIES = new Map([
  ["AElig", 0xc6],
  ["Aacute", 0xc1],
  ["Acirc", 0xc2],
  ["Agrave", 0xc0],
  ["Aring", 0xc5],
  ["Atilde", 0xc3],
  ["Auml", 0xc4],
  ["Ccedil", 0xc7],
  ["ETH", 0xd0],
  ["Eacute", 0xc9],
  ["Ecirc", 0xca],
  ["Egrave", 0xc8],
  ["Euml", 0xcb],
  ["Iacute", 0xcd],
  ["Icirc", 0xce],
  ["Igrave", 0xcc],
  ["Iuml", 0xcf],
  ["Ntilde", 0xd1],
  ["Oacute", 0xd3],
  ["Ocirc", 0xd4],
  ["Ograve", 0xd2],
  ["Oslash", 0xd8],
  ["Otilde", 0xd5],
  ["Ouml", 0xd6],
  ["THORN", 0xde],
  ["Uacute", 0xda],
  ["Ucirc", 0xdb],
  ["Ugrave", 0xd9],
  ["Uuml", 0xdc],
  ["Yacute", 0xdd],
  ["aacute", 0xe1],
  ["acirc", 0xe2],
  ["aelig", 0xe6],
  ["agrave", 0xe0],
  ["aring", 0xe5],
  ["atilde", 0xe3],
  ["auml", 0xe4],
  ["amp", 0x26],
  ["apos", 0x27],
  ["bull", 0x2022],
  ["brvbar", 0xa6],
  ["ccedil", 0xe7],
  ["cedil", 0xb8],
  ["cent", 0xa2],
  ["copy", 0xa9],
  ["deg", 0xb0],
  ["curren", 0xa4],
  ["divide", 0xf7],
  ["emsp", 0x2003],
  ["ensp", 0x2002],
  ["euro", 0x20ac],
  ["eacute", 0xe9],
  ["ecirc", 0xea],
  ["egrave", 0xe8],
  ["eth", 0xf0],
  ["euml", 0xeb],
  ["frac12", 0xbd],
  ["frac14", 0xbc],
  ["frac34", 0xbe],
  ["gt", 0x3e],
  ["iacute", 0xed],
  ["icirc", 0xee],
  ["iexcl", 0xa1],
  ["igrave", 0xec],
  ["iquest", 0xbf],
  ["iuml", 0xef],
  ["hellip", 0x2026],
  ["laquo", 0xab],
  ["ldquo", 0x201c],
  ["lsquo", 0x2018],
  ["lt", 0x3c],
  ["mdash", 0x2014],
  ["macr", 0xaf],
  ["micro", 0xb5],
  ["middot", 0xb7],
  ["nbsp", 0xa0],
  ["ndash", 0x2013],
  ["ntilde", 0xf1],
  ["oacute", 0xf3],
  ["ocirc", 0xf4],
  ["ograve", 0xf2],
  ["ordf", 0xaa],
  ["ordm", 0xba],
  ["oslash", 0xf8],
  ["otilde", 0xf5],
  ["ouml", 0xf6],
  ["para", 0xb6],
  ["pound", 0xa3],
  ["plusmn", 0xb1],
  ["quot", 0x22],
  ["raquo", 0xbb],
  ["rdquo", 0x201d],
  ["reg", 0xae],
  ["rsquo", 0x2019],
  ["sect", 0xa7],
  ["shy", 0xad],
  ["szlig", 0xdf],
  ["thorn", 0xfe],
  ["sup1", 0xb9],
  ["sup2", 0xb2],
  ["sup3", 0xb3],
  ["thinsp", 0x2009],
  ["times", 0xd7],
  ["trade", 0x2122],
  ["uacute", 0xfa],
  ["ucirc", 0xfb],
  ["ugrave", 0xf9],
  ["uml", 0xa8],
  ["uuml", 0xfc],
  ["yacute", 0xfd],
  ["yuml", 0xff],
  ["yen", 0xa5],
]);

class HtmlTree {
  readonly root: HtmlElement = {
    tag: "#document",
    svg: false,
    attributes: new Map(),
    children: [],
    parent: undefined,
  };
  readonly xml: boolean;
  readonly #open: Array<HtmlElement> = [];

  constructor(xml: boolean) {
    this.xml = xml;
  }

  text(value: string): void {
    this.#current().children.push(value);
  }

  /** omp keeps comments in the tree and counts their text in `textContent`. */
  comment(data: string): void {
    this.#append(COMMENT_TAG, new Map(), false).children.push(data);
  }

  start(tag: StartTag): HtmlElement {
    this.#closeOptional(tag.name);
    const svg = this.#current().svg || tag.name === "svg";
    return this.#append(tag.name, tag.attributes, svg);
  }

  open(element: HtmlElement): void {
    this.#open.push(element);
  }

  end(tag: string): void {
    const at = this.#open.findLastIndex((element) => element.tag === tag);
    if (at >= 0) this.#open.length = at;
  }

  #append(tag: string, attributes: ReadonlyMap<string, string>, svg: boolean): HtmlElement {
    const parent = this.#current();
    const element: HtmlElement = { tag, svg, attributes, children: [], parent };
    parent.children.push(element);
    return element;
  }

  #current(): HtmlElement {
    return this.#open.at(-1) ?? this.root;
  }

  #closeOptional(incoming: string): void {
    const current = this.#open.at(-1);
    if (current === undefined) return;
    if (current.tag === "p" && P_CLOSERS.has(incoming)) {
      this.#open.pop();
      return;
    }
    if (CLOSED_BY.get(incoming)?.has(current.tag) === true) this.#open.pop();
  }
}

function parseHtmlDocument(html: string): HtmlElement {
  const tree = new HtmlTree(XML_FEED.test(html));
  let index = 0;
  while (index < html.length) index = readNode(html, index, tree);
  return tree.root;
}

function readNode(html: string, index: number, tree: HtmlTree): number {
  if (html.startsWith("<!--", index)) return readComment(html, index, tree);
  if (html.charAt(index) !== "<") return readText(html, index, tree);
  if (html.startsWith("<![CDATA[", index)) return readCdata(html, index, tree);
  const next = html.charAt(index + 1);
  if (next === "/") return readEndTag(html, index, tree);
  if (next === "!" || next === "?") return skipPast(html, index + 2, ">");
  return readStartTag(html, index, tree);
}

function readText(html: string, index: number, tree: HtmlTree): number {
  const end = html.indexOf("<", index);
  const next = end < 0 ? html.length : end;
  tree.text(decodeEntities(html.slice(index, next)));
  return next;
}

function readComment(html: string, index: number, tree: HtmlTree): number {
  const end = html.indexOf("-->", index + 4);
  tree.comment(html.slice(index + 4, end < 0 ? html.length : end));
  return end < 0 ? html.length : end + 3;
}

function readCdata(html: string, index: number, tree: HtmlTree): number {
  const end = html.indexOf("]]>", index + 9);
  tree.text(html.slice(index + 9, end < 0 ? html.length : end));
  return end < 0 ? html.length : end + 3;
}

function skipPast(html: string, from: number, marker: string): number {
  const end = html.indexOf(marker, from);
  return end < 0 ? html.length : end + marker.length;
}

function readEndTag(html: string, index: number, tree: HtmlTree): number {
  const end = findTagEnd(html, index + 2);
  const name =
    html
      .slice(index + 2, end)
      .trim()
      .split(/\s/, 1)[0] ?? "";
  tree.end(name.toLowerCase());
  return end + 1;
}

/**
 * Checked before the scan for `>`: without it every stray `<` rescans to the next `>`, which is
 * quadratic on the server's event loop (omp's own parser.ts scans the same way).
 */
function opensTagName(html: string, index: number): boolean {
  return matchAt(TAG_NAME, html, skipSpace(html, index + 1)) !== undefined;
}

function readStrayAngle(index: number, tree: HtmlTree): number {
  tree.text("<");
  return index + 1;
}

function readStartTag(html: string, index: number, tree: HtmlTree): number {
  if (!opensTagName(html, index)) return readStrayAngle(index, tree);
  const end = findTagEnd(html, index + 1);
  const tag = parseStartTag(html.slice(index + 1, end));
  if (tag === undefined) return readStrayAngle(index, tree);
  const element = tree.start(tag);
  const raw = tag.selfClosing ? undefined : rawText(element, tree.xml);
  if (raw !== undefined) return readRawText(html, end + 1, element, raw);
  if (!isVoid(element, tag.selfClosing, tree.xml)) tree.open(element);
  return end + 1;
}

/** The `>` ending a tag, past quoted text; with none, the last character, so the tag takes the rest. */
function findTagEnd(html: string, start: number): number {
  let quote = "";
  for (let index = start; index < html.length; index++) {
    const character = html.charAt(index);
    if (quote !== "") {
      if (character === quote) quote = "";
    } else if (character === '"' || character === "'") {
      quote = character;
    } else if (character === ">") {
      return index;
    }
  }
  return html.length - 1;
}

/** `script` and `style` keep their text as written; an HTML `textarea` or `title` decodes it. */
function rawText(element: HtmlElement, xml: boolean): RawText | undefined {
  return RAW_TEXT.get(element.tag) ?? (xml || element.svg ? undefined : RCDATA.get(element.tag));
}

function isVoid(element: HtmlElement, selfClosing: boolean, xml: boolean): boolean {
  if (xml) return selfClosing;
  return VOID_TAGS.has(element.tag) || (element.svg && selfClosing);
}

function readRawText(html: string, from: number, element: HtmlElement, raw: RawText): number {
  raw.end.lastIndex = from;
  const match = raw.end.exec(html);
  const text = html.slice(from, match === null ? html.length : match.index);
  element.children.push(raw.decode ? decodeEntities(text) : text);
  return match === null ? html.length : raw.end.lastIndex;
}

function parseStartTag(source: string): StartTag | undefined {
  const start = skipSpace(source, 0);
  const name = matchAt(TAG_NAME, source, start)?.toLowerCase();
  if (name === undefined) return undefined;
  const attributes = new Map<string, string>();
  let index = start + name.length;
  while (index < source.length) {
    index = skipSpace(source, index);
    if (source.charAt(index) === "/") return { name, attributes, selfClosing: true };
    const attribute = readAttribute(source, index);
    if (attribute === undefined) break;
    if (!attributes.has(attribute.name)) {
      attributes.set(attribute.name, decodeEntities(attribute.value));
    }
    index = attribute.end;
  }
  return { name, attributes, selfClosing: false };
}

function readAttribute(source: string, index: number) {
  const name = matchAt(ATTRIBUTE_NAME, source, index)?.toLowerCase();
  if (name === undefined) return undefined;
  const afterName = skipSpace(source, index + name.length);
  if (source.charAt(afterName) !== "=") return { name, value: "", end: afterName };
  return { name, ...attributeValue(source, skipSpace(source, afterName + 1)) };
}

/** A quote left open runs to the end of the tag. */
function attributeValue(source: string, index: number) {
  const quote = source.charAt(index);
  if (quote !== '"' && quote !== "'") {
    const value = matchAt(UNQUOTED_VALUE, source, index) ?? "";
    return { value, end: index + value.length };
  }
  const close = source.indexOf(quote, index + 1);
  return close < 0
    ? { value: source.slice(index + 1), end: source.length }
    : { value: source.slice(index + 1, close), end: close + 1 };
}

function skipSpace(source: string, index: number): number {
  let at = index;
  while (at < source.length && SPACE.test(source.charAt(at))) at++;
  return at;
}

function matchAt(pattern: RegExp, source: string, index: number): string | undefined {
  pattern.lastIndex = index;
  return pattern.exec(source)?.[0];
}

function decodeEntities(text: string): string {
  return text.includes("&") ? text.replace(ENTITY, decodeEntity) : text;
}

function decodeEntity(whole: string, entity: string): string {
  if (!entity.startsWith("#")) return namedEntity(entity) ?? whole;
  const hexadecimal = entity.charAt(1).toLowerCase() === "x";
  const code = Number.parseInt(entity.slice(hexadecimal ? 2 : 1), hexadecimal ? 16 : 10);
  return isScalarValue(code) ? String.fromCodePoint(code) : REPLACEMENT_CHARACTER;
}

function namedEntity(name: string): string | undefined {
  const code = NAMED_ENTITIES.get(name) ?? NAMED_ENTITIES.get(name.toLowerCase());
  return code === undefined ? undefined : String.fromCodePoint(code);
}

function isScalarValue(code: number): boolean {
  return code > 0 && code <= 0x10ffff && (code < 0xd800 || code > 0xdfff);
}

function attribute(element: HtmlElement, name: string): string | undefined {
  return element.attributes.get(name);
}

function hasAttribute(element: HtmlElement, name: string): boolean {
  return element.attributes.has(name);
}

function childElements(element: HtmlElement): ReadonlyArray<HtmlElement> {
  return element.children.filter((child): child is HtmlElement => typeof child !== "string");
}

/** omp keeps an HTML `<template>`'s markup out of the tree it searches. */
function* descendants(element: HtmlElement): Generator<HtmlElement> {
  for (const child of childElements(element)) {
    yield child;
    if (child.tag !== "template" || child.svg) yield* descendants(child);
  }
}

function findAll(element: HtmlElement, tags: ReadonlySet<string>): ReadonlyArray<HtmlElement> {
  return [...descendants(element)].filter((candidate) => tags.has(candidate.tag));
}

function findFirst(element: HtmlElement, tags: ReadonlySet<string>): HtmlElement | undefined {
  for (const candidate of descendants(element)) if (tags.has(candidate.tag)) return candidate;
  return undefined;
}

function closest(element: HtmlElement, tag: string): HtmlElement | undefined {
  let node: HtmlElement | undefined = element;
  while (node !== undefined && node.tag !== tag) node = node.parent;
  return node;
}

function contains(ancestor: HtmlElement, element: HtmlElement): boolean {
  for (let node: HtmlElement | undefined = element; node !== undefined; node = node.parent) {
    if (node === ancestor) return true;
  }
  return false;
}

/** Comments and template markup count, as in omp. */
function textContent(node: HtmlNode): string {
  return typeof node === "string" ? node : node.children.map(textContent).join("");
}
