import fs from "node:fs";
import path from "node:path";
import type { AgentflowYamlMapping, AgentflowYamlValue } from "./workflow";

export function matchesPolicyGlob(candidate: string, sourcePattern: string): boolean {
  const expanded = normalizedPolicyPatterns(sourcePattern);
  if (expanded === undefined) return false;

  return expanded.some((entry) => {
    const expression = globExpression(entry);
    const regex = expression === undefined ? undefined : policyGlobRegex(expression);
    return regex?.test(candidate) ?? false;
  });
}

export function isSupportedPolicyGlob(sourcePattern: string): boolean {
  const expanded = normalizedPolicyPatterns(sourcePattern);
  return expanded !== undefined && expanded.every((entry) => {
    const expression = globExpression(entry);
    return expression !== undefined && policyGlobRegex(expression) !== undefined;
  });
}

export function policyGlobCanMatchDescendant(candidate: string, sourcePattern: string): boolean {
  const patterns = normalizedPolicyPatterns(sourcePattern);
  if (patterns === undefined) return false;
  const subtree = compileLiteralSubtreeAutomaton(candidate);
  return patterns.some((pattern) => globAutomataIntersect(subtree, compileGlobAutomaton(pattern)));
}

export function policyGlobCoversSubtree(candidate: string, sourcePattern: string): boolean {
  return policyGlobsCoverSubtree(candidate, [sourcePattern]);
}

export function policyGlobsCoverSubtree(candidate: string, sourcePatterns: string[]): boolean {
  const expanded: string[] = [];
  for (const sourcePattern of sourcePatterns) {
    const patterns = normalizedPolicyPatterns(sourcePattern);
    if (patterns === undefined) return false;
    expanded.push(...patterns);
  }
  if (expanded.length === 0) return false;
  const subtree = compileLiteralSubtreeAutomaton(candidate);
  return !globAutomataHaveUnexcludedIntersection([subtree], [compileGlobUnion(expanded)]);
}

export function policyGlobsMayOverlap(left: string, right: string): boolean {
  left = normalizeOverlapPattern(left);
  right = normalizeOverlapPattern(right);

  if (left === right) return true;
  if (/[{\[]/.test(left) || /[{\[]/.test(right)) return true;
  if (!left.includes("**") && !right.includes("**") && overlapPathDepth(left) !== overlapPathDepth(right)) {
    return false;
  }

  const leftPrefix = overlapScopePrefix(left);
  const rightPrefix = overlapScopePrefix(right);
  const prefixesCanOverlap = leftPrefix.length === 0 || rightPrefix.length === 0 ||
    overlapPrefixCanContain(left, leftPrefix, rightPrefix) ||
    overlapPrefixCanContain(right, rightPrefix, leftPrefix);
  if (!prefixesCanOverlap) return false;

  const leftSuffix = overlapScopeSuffix(left);
  const rightSuffix = overlapScopeSuffix(right);
  if (leftSuffix && rightSuffix && !leftSuffix.endsWith(rightSuffix) && !rightSuffix.endsWith(leftSuffix)) {
    return false;
  }
  return overlapHasGlob(left) || overlapHasGlob(right);
}

export function policyGlobsIntersect(left: string, right: string): boolean {
  const leftPatterns = normalizedPolicyPatterns(left);
  const rightPatterns = normalizedPolicyPatterns(right);
  if (leftPatterns === undefined || rightPatterns === undefined) return false;

  return leftPatterns.some((leftPattern) => rightPatterns.some((rightPattern) =>
    globAutomataIntersect(compileGlobAutomaton(leftPattern), compileGlobAutomaton(rightPattern))
  ));
}

export function policyGlobLayersHaveWritablePath(includeLayers: string[][], exclusions: string[]): boolean {
  const requiredChoices = includeLayers
    .filter((layer) => layer.length > 0)
    .map((layer) => layer.flatMap((pattern) =>
      isSupportedPolicyGlob(pattern) ? normalizedPolicyPatterns(pattern) ?? [] : []
    ));
  if (requiredChoices.length === 0 || requiredChoices.some((choices) => choices.length === 0)) return false;
  const excludedAutomata = exclusions.flatMap((pattern) =>
    (isSupportedPolicyGlob(pattern) ? normalizedPolicyPatterns(pattern) ?? [] : []).map(compileGlobAutomaton)
  );

  return globAutomataHaveUnexcludedIntersection(requiredChoices.map(compileGlobUnion), excludedAutomata);
}

interface GlobTransition {
  destination: number;
  predicate: GlobCharacterPredicate;
}

interface GlobAutomaton {
  accepting: number;
  epsilon: number[][];
  transitions: GlobTransition[][];
}

interface GlobCharacterPredicate {
  key: string;
  boundaries: number[];
  matches: (character: string) => boolean;
}

function compileGlobAutomaton(pattern: string): GlobAutomaton {
  const epsilon: number[][] = [[]];
  const transitions: GlobTransition[][] = [[]];
  let state = 0;
  const addState = (): number => {
    epsilon.push([]);
    transitions.push([]);
    return epsilon.length - 1;
  };
  const addTransition = (predicate: GlobCharacterPredicate): void => {
    const destination = addState();
    transitions[state].push({ destination, predicate });
    state = destination;
  };

  for (let index = 0; index < pattern.length; index += 1) {
    const character = pattern[index];
    if (character === "*" && pattern[index + 1] === "*" && pattern[index + 2] === "/") {
      const destination = addState();
      epsilon[state].push(destination);
      const consumed = addState();
      transitions[state].push({ destination: consumed, predicate: anyGlobCharacter() });
      transitions[state].push({ destination, predicate: literalGlobCharacter("/") });
      transitions[consumed].push({ destination: consumed, predicate: anyGlobCharacter() });
      transitions[consumed].push({ destination, predicate: literalGlobCharacter("/") });
      state = destination;
      index += 2;
    } else if (character === "*" && pattern[index + 1] === "*") {
      const destination = addState();
      epsilon[state].push(destination);
      transitions[state].push({ destination: state, predicate: anyGlobCharacter() });
      state = destination;
      index += 1;
    } else if (character === "*") {
      const destination = addState();
      epsilon[state].push(destination);
      transitions[state].push({ destination: state, predicate: nonSlashGlobCharacter() });
      state = destination;
    } else if (character === "?") {
      addTransition(nonSlashGlobCharacter());
    } else if (character === "[") {
      const closing = pattern.indexOf("]", index + 1);
      if (closing === -1) {
        addTransition(neverGlobCharacter());
        break;
      }
      const content = pattern.slice(index + 1, closing);
      addTransition(content.length === 0 ? neverGlobCharacter() : classGlobCharacter(content));
      index = closing;
    } else {
      addTransition(literalGlobCharacter(character));
    }
  }

  return { accepting: state, epsilon, transitions };
}

function compileLiteralSubtreeAutomaton(candidate: string): GlobAutomaton {
  const epsilon: number[][] = [[]];
  const transitions: GlobTransition[][] = [[]];
  let state = 0;
  const addState = (): number => {
    epsilon.push([]);
    transitions.push([]);
    return epsilon.length - 1;
  };
  for (let index = 0; index < candidate.length; index += 1) {
    const destination = addState();
    transitions[state].push({ destination, predicate: literalGlobCharacter(candidate[index]) });
    state = destination;
  }
  const descendant = addState();
  transitions[state].push({ destination: descendant, predicate: literalGlobCharacter("/") });
  state = descendant;
  const accepting = addState();
  epsilon[state].push(accepting);
  transitions[state].push({ destination: state, predicate: anyGlobCharacter() });
  return { accepting, epsilon, transitions };
}

function compileGlobUnion(patterns: string[]): GlobAutomaton {
  const epsilon: number[][] = [[]];
  const transitions: GlobTransition[][] = [[]];
  const acceptingStates: number[] = [];

  for (const pattern of patterns) {
    const automaton = compileGlobAutomaton(pattern);
    const offset = epsilon.length;
    epsilon[0].push(offset);
    automaton.epsilon.forEach((destinations) => epsilon.push(destinations.map((state) => state + offset)));
    automaton.transitions.forEach((entries) => transitions.push(entries.map((entry) => ({
      destination: entry.destination + offset,
      predicate: entry.predicate
    }))));
    acceptingStates.push(automaton.accepting + offset);
  }

  const accepting = epsilon.length;
  epsilon.push([]);
  transitions.push([]);
  acceptingStates.forEach((state) => epsilon[state].push(accepting));
  return { accepting, epsilon, transitions };
}

function globAutomataIntersect(left: GlobAutomaton, right: GlobAutomaton): boolean {
  const pending: Array<[number, number]> = [[0, 0]];
  const visited = new Set<string>();
  while (pending.length > 0) {
    const [leftState, rightState] = pending.pop()!;
    const key = `${leftState}:${rightState}`;
    if (visited.has(key)) continue;
    visited.add(key);
    if (leftState === left.accepting && rightState === right.accepting) return true;

    left.epsilon[leftState].forEach((destination) => pending.push([destination, rightState]));
    right.epsilon[rightState].forEach((destination) => pending.push([leftState, destination]));
    for (const leftTransition of left.transitions[leftState]) {
      for (const rightTransition of right.transitions[rightState]) {
        if (globPredicatesIntersect(leftTransition.predicate, rightTransition.predicate)) {
          pending.push([leftTransition.destination, rightTransition.destination]);
        }
      }
    }
  }
  return false;
}

function globAutomataHaveUnexcludedIntersection(
  required: GlobAutomaton[],
  excluded: GlobAutomaton[]
): boolean {
  const automata = [...required, ...excluded];
  const initial = automata.map((automaton) => epsilonClosure(automaton, [0]));
  const pending: Array<{ consumed: boolean; states: number[][] }> = [{ consumed: false, states: initial }];
  const visited = new Set<string>();
  const alphabet = globAutomataAlphabet(automata);

  while (pending.length > 0) {
    const { consumed, states } = pending.pop()!;
    const key = `${consumed ? "1" : "0"}:${states.map((state) => state.join(",")).join(":")}`;
    if (visited.has(key)) continue;
    visited.add(key);
    const requiredStates = states.slice(0, required.length);
    const excludedStates = states.slice(required.length);
    if (consumed && requiredStates.every((state, index) => state.includes(required[index].accepting)) &&
        excludedStates.every((state, index) => !state.includes(excluded[index].accepting))) {
      return true;
    }

    for (const character of alphabet) {
      const next = states.map((state, index) => advanceGlobAutomaton(automata[index], state, character));
      if (next.slice(0, required.length).every((state) => state.length > 0)) {
        pending.push({ consumed: true, states: next });
      }
    }
  }
  return false;
}

function epsilonClosure(automaton: GlobAutomaton, startingStates: number[]): number[] {
  const closure = new Set(startingStates);
  const pending = [...startingStates];
  while (pending.length > 0) {
    const state = pending.pop()!;
    for (const destination of automaton.epsilon[state]) {
      if (!closure.has(destination)) {
        closure.add(destination);
        pending.push(destination);
      }
    }
  }
  return [...closure].sort((left, right) => left - right);
}

function advanceGlobAutomaton(automaton: GlobAutomaton, states: number[], character: string): number[] {
  const destinations = new Set<number>();
  for (const state of states) {
    for (const transition of automaton.transitions[state]) {
      if (transition.predicate.matches(character)) destinations.add(transition.destination);
    }
  }
  return epsilonClosure(automaton, [...destinations]);
}

const globAlphabetCache = new Map<string, string[]>();

function globAutomataAlphabet(automata: GlobAutomaton[]): string[] {
  const predicates = new Map<string, GlobCharacterPredicate>();
  automata.forEach((automaton) => automaton.transitions.forEach((transitions) => transitions.forEach((transition) => {
    predicates.set(transition.predicate.key, transition.predicate);
  })));
  const ordered = [...predicates.values()].sort((left, right) => left.key.localeCompare(right.key));
  const cacheKey = ordered.map((predicate) => predicate.key).join("\0");
  const cached = globAlphabetCache.get(cacheKey);
  if (cached !== undefined) return cached;
  const alphabet = predicateRepresentatives(ordered);
  globAlphabetCache.set(cacheKey, alphabet);
  return alphabet;
}

const predicateIntersectionCache = new Map<string, boolean>();

function globPredicatesIntersect(left: GlobCharacterPredicate, right: GlobCharacterPredicate): boolean {
  const key = left.key < right.key ? `${left.key}\0${right.key}` : `${right.key}\0${left.key}`;
  const cached = predicateIntersectionCache.get(key);
  if (cached !== undefined) return cached;

  for (const character of predicateRepresentatives([left, right])) {
    if (left.matches(character) && right.matches(character)) {
      predicateIntersectionCache.set(key, true);
      return true;
    }
  }
  predicateIntersectionCache.set(key, false);
  return false;
}

function anyGlobCharacter(): GlobCharacterPredicate {
  return {
    key: "any",
    boundaries: [0, 0x10000],
    matches: () => true
  };
}

function nonSlashGlobCharacter(): GlobCharacterPredicate {
  return { key: "non-slash", boundaries: [0, 47, 48, 0x10000], matches: (character) => character !== "/" };
}

function literalGlobCharacter(literal: string): GlobCharacterPredicate {
  const code = literal.charCodeAt(0);
  return {
    key: `literal:${code}`,
    boundaries: [0, code, code + 1, 0x10000],
    matches: (character) => character === literal
  };
}

function neverGlobCharacter(): GlobCharacterPredicate {
  return { key: "never", boundaries: [0, 0x10000], matches: () => false };
}

const classPredicateCache = new Map<string, GlobCharacterPredicate>();

function classGlobCharacter(content: string): GlobCharacterPredicate {
  const negated = content.startsWith("!");
  const members = (negated ? content.slice(1) : content).replaceAll("\\", "\\\\");
  let expression: RegExp;
  try {
    expression = new RegExp(`^[${negated ? "^" : ""}${members}]$`);
  } catch {
    return neverGlobCharacter();
  }
  const key = `class:${expression.source}`;
  const cached = classPredicateCache.get(key);
  if (cached !== undefined) return cached;
  const matches = (character: string): boolean => character !== "/" && expression.test(character);
  const boundaries = predicateBoundaries(matches);
  const predicate = { key, boundaries, matches };
  classPredicateCache.set(key, predicate);
  return predicate;
}

function predicateBoundaries(matches: (character: string) => boolean): number[] {
  const boundaries = [0];
  let previous = matches(String.fromCharCode(0));
  for (let code = 1; code <= 0xffff; code += 1) {
    const current = matches(String.fromCharCode(code));
    if (current !== previous) boundaries.push(code);
    previous = current;
  }
  boundaries.push(0x10000);
  return boundaries;
}

function predicateRepresentatives(predicates: GlobCharacterPredicate[]): string[] {
  const boundaries = [...new Set(predicates.flatMap((predicate) => predicate.boundaries))]
    .filter((code) => code >= 0 && code < 0x10000)
    .sort((left, right) => left - right);
  return boundaries.map((code) => String.fromCharCode(code));
}

function normalizeOverlapPattern(pattern: string): string {
  const segments: string[] = [];
  for (const segment of pattern.replaceAll("\\", "/").split("/")) {
    if (segment.length === 0 || segment === ".") continue;
    if (segment === ".." && segments.length > 0 && segments.at(-1) !== "**") segments.pop();
    else segments.push(segment);
  }
  return segments.join("/");
}

function overlapPrefixCanContain(pattern: string, prefix: string, candidate: string): boolean {
  if (!candidate.startsWith(prefix)) return false;
  if (candidate.length === prefix.length || overlapHasGlob(pattern)) return true;
  return prefix.endsWith("/") || candidate[prefix.length] === "/";
}

function overlapHasGlob(pattern: string): boolean {
  return /[?*{\[]/.test(pattern);
}

function overlapPathDepth(pattern: string): number {
  return pattern.split("/").length;
}

function overlapScopePrefix(pattern: string): string {
  const wildcardIndex = pattern.search(/[?*{\[]/);
  return wildcardIndex === -1 ? pattern : pattern.slice(0, wildcardIndex);
}

function overlapScopeSuffix(pattern: string): string {
  const wildcardIndex = Math.max(pattern.lastIndexOf("*"), pattern.lastIndexOf("?"), pattern.lastIndexOf("]"));
  return wildcardIndex === -1 ? pattern : pattern.slice(wildcardIndex + 1);
}

function normalizedPolicyPatterns(sourcePattern: string): string[] | undefined {
  if (!nonEmptyString(sourcePattern)) return undefined;
  const expanded = expandBraces(sourcePattern.trim().replaceAll("\\", "/"));
  if (expanded === undefined) return undefined;
  const normalized = expanded.map(normalizeRepoPattern);
  return normalized.every((entry): entry is string => entry !== undefined) ? normalized : undefined;
}

function policyGlobRegex(expression: string): RegExp | undefined {
  try {
    return new RegExp(`^${expression}$`);
  } catch {
    return undefined;
  }
}

function globExpression(pattern: string): string | undefined {
  let expression = "^";
  for (let index = 0; index < pattern.length; index += 1) {
    const character = pattern[index];
    if (character === "*" && pattern[index + 1] === "*") {
      if (pattern[index + 2] === "/") {
        expression += "(?:[\\s\\S]*/)?";
        index += 2;
      } else {
        expression += "[\\s\\S]*";
        index += 1;
      }
    } else if (character === "*") {
      expression += "[^/]*";
    } else if (character === "?") {
      expression += "[^/]";
    } else if (character === "[") {
      const closing = pattern.indexOf("]", index + 1);
      if (closing === -1) return undefined;
      const content = pattern.slice(index + 1, closing);
      if (content.length === 0 || content.includes("/") || content.includes("[")) return undefined;
      const negated = content.startsWith("!");
      const members = (negated ? content.slice(1) : content).replaceAll("\\", "\\\\");
      if (members.length === 0) return undefined;
      expression += `(?=[^/])[${negated ? "^" : ""}${members}]`;
      index = closing;
    } else {
      expression += character.replace(/[\\^$.*+?()[\]{}|]/g, "\\$&");
    }
  }
  return expression.slice(1);
}

function expandBraces(pattern: string, limit = 256): string[] | undefined {
  const opening = pattern.indexOf("{");
  if (opening === -1) return pattern.includes("}") ? undefined : [pattern];

  let depth = 0;
  let closing = -1;
  for (let index = opening; index < pattern.length; index += 1) {
    if (pattern[index] === "{") depth += 1;
    if (pattern[index] === "}") depth -= 1;
    if (depth < 0) return undefined;
    if (depth === 0) {
      closing = index;
      break;
    }
  }
  if (closing === -1) return undefined;

  const body = pattern.slice(opening + 1, closing);
  const alternatives = splitBraceAlternatives(body);
  if (alternatives === undefined || alternatives.length < 2) return undefined;

  const expanded: string[] = [];
  for (const alternative of alternatives) {
    const nested = expandBraces(`${pattern.slice(0, opening)}${alternative}${pattern.slice(closing + 1)}`, limit);
    if (nested === undefined || expanded.length + nested.length > limit) return undefined;
    expanded.push(...nested);
  }
  return expanded;
}

function splitBraceAlternatives(value: string): string[] | undefined {
  const alternatives: string[] = [];
  let depth = 0;
  let start = 0;
  for (let index = 0; index < value.length; index += 1) {
    if (value[index] === "{") depth += 1;
    if (value[index] === "}") depth -= 1;
    if (depth < 0) return undefined;
    if (value[index] === "," && depth === 0) {
      alternatives.push(value.slice(start, index));
      start = index + 1;
    }
  }
  if (depth !== 0) return undefined;
  alternatives.push(value.slice(start));
  return alternatives.every((entry) => entry.length > 0) ? alternatives : undefined;
}

export function normalizeRepoPath(value: string): string | undefined {
  if (!nonEmptyString(value)) return undefined;
  if (value !== value.trim() || value.includes("\\") || hasControlCharacter(value)) return undefined;
  const normalized = value;
  if (normalized.startsWith("/") || /^[A-Za-z]:/.test(normalized)) return undefined;
  const resolved = path.posix.normalize(normalized);
  if (resolved === "." || resolved === ".." || resolved.startsWith("../")) return undefined;
  return resolved;
}

export function isPolicyRootDirectory(value: unknown): value is string {
  if (!nonEmptyString(value)) return false;
  try {
    return fs.statSync(fs.realpathSync(value)).isDirectory();
  } catch {
    return false;
  }
}

export function resolveScopedRepoPath(rootPath: string, value: string): string | undefined {
  const normalized = normalizeRepoPath(value);
  if (normalized === undefined || !nonEmptyString(rootPath)) return undefined;

  try {
    const root = fs.realpathSync(rootPath);
    if (!fs.statSync(root).isDirectory()) return undefined;
    const segments = normalized.split("/");
    let current = root;
    for (let index = 0; index < segments.length; index += 1) {
      const candidate = path.join(current, segments[index]);
      const relative = path.relative(root, candidate);
      if (relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) return undefined;
      let currentStat: fs.Stats;
      try {
        currentStat = fs.lstatSync(candidate);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") {
          const existing = path.relative(root, current).split(path.sep).filter(Boolean);
          return [...existing, ...segments.slice(index)].join("/");
        }
        return undefined;
      }
      if (currentStat.isSymbolicLink()) return undefined;
      const resolved = fs.realpathSync(candidate);
      const resolvedRelative = path.relative(root, resolved);
      if (resolvedRelative === ".." || resolvedRelative.startsWith(`..${path.sep}`) || path.isAbsolute(resolvedRelative)) {
        return undefined;
      }
      current = resolved;
    }
    return path.relative(root, current).split(path.sep).join("/");
  } catch {
    return undefined;
  }
}

export function normalizeRepoPattern(value: string): string | undefined {
  if (!nonEmptyString(value) || hasControlCharacter(value)) return undefined;
  const normalized = value.trim().replaceAll("\\", "/");
  if (normalized.startsWith("/") || /^[A-Za-z]:/.test(normalized)) return undefined;
  const segments: string[] = [];
  for (const segment of normalized.split("/")) {
    if (segment.length === 0 || segment === ".") continue;
    if (segment === "..") {
      if (segments.length === 0 || /[*?{\[]/.test(segments.at(-1) ?? "")) return undefined;
      segments.pop();
    } else {
      segments.push(segment);
    }
  }
  return segments.length === 0 ? undefined : segments.join("/");
}

export function quotePolicyValue(value: string): string {
  return JSON.stringify(value).replace(/[\u007f-\u009f\u2028\u2029]/g, (character) =>
    `\\u${character.codePointAt(0)?.toString(16).padStart(4, "0")}`
  );
}

function hasControlCharacter(value: string): boolean {
  return /[\u0000-\u001f\u007f-\u009f]/.test(value);
}

export function mapping(value: AgentflowYamlValue | undefined): AgentflowYamlMapping | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as AgentflowYamlMapping
    : undefined;
}

export function stringList(value: AgentflowYamlValue | undefined): string[] {
  return Array.isArray(value) ? value.filter((entry): entry is string => nonEmptyString(entry)) : [];
}

export function nonEmptyStringList(value: AgentflowYamlValue | undefined): boolean {
  return Array.isArray(value) && value.length > 0 && value.every((entry) => nonEmptyString(entry));
}

export function nonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

export function positiveFinite(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value > 0;
}

export function nonNegativeFinite(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0;
}
