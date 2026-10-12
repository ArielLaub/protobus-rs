//! Matches dotted topics against AMQP topic patterns, as the broker does: `*`
//! is exactly one word and `#` zero or more.

use std::collections::HashMap;

/// A set of values keyed by topic pattern. Several values may share a pattern;
/// [`matches`](Trie::matches) returns each matching value once, in the order
/// the patterns were added.
#[derive(Debug)]
pub struct Trie<T> {
    nodes: Vec<Node>,
    values: Vec<T>,
}

#[derive(Debug)]
struct Node {
    word: String,
    values: Vec<usize>,
    children: HashMap<String, usize>,
    order: Vec<usize>,
}

impl Node {
    fn new(word: &str) -> Self {
        Node {
            word: word.to_string(),
            values: Vec::new(),
            children: HashMap::new(),
            order: Vec::new(),
        }
    }

    fn wildcard(&self) -> bool {
        self.word == "*" || self.word == "#"
    }

    fn super_wildcard(&self) -> bool {
        self.word == "#"
    }
}

impl<T> Default for Trie<T> {
    fn default() -> Self {
        Trie {
            nodes: vec![Node::new("")],
            values: Vec::new(),
        }
    }
}

impl<T> Trie<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, pattern: &str, value: T) {
        let mut node = 0;
        for word in pattern.split('.') {
            node = match self.nodes[node].children.get(word) {
                Some(&child) => child,
                None => {
                    let child = self.nodes.len();
                    self.nodes.push(Node::new(word));
                    self.nodes[node].children.insert(word.to_string(), child);
                    self.nodes[node].order.push(child);
                    child
                }
            };
        }
        // Only the node the pattern ends on carries the value.
        let index = self.values.len();
        self.values.push(value);
        self.nodes[node].values.push(index);
    }

    pub fn matches(&self, topic: &str) -> Vec<&T> {
        let words: Vec<&str> = topic.split('.').collect();
        let mut ends = Vec::new();
        for &child in &self.nodes[0].order {
            self.collect(child, &words, 0, &mut ends);
        }
        let mut seen = vec![false; self.values.len()];
        let mut results = Vec::new();
        for node in ends {
            for &v in &self.nodes[node].values {
                if !seen[v] {
                    seen[v] = true;
                    results.push(&self.values[v]);
                }
            }
        }
        results
    }

    fn end(ends: &mut Vec<usize>, node: usize) {
        if !ends.contains(&node) {
            ends.push(node);
        }
    }

    fn collect(&self, index: usize, words: &[&str], i: usize, ends: &mut Vec<usize>) {
        let node = &self.nodes[index];
        if node.super_wildcard() {
            // '#' consumes zero words: hand words[i..] to the children; or one more, staying on '#'.
            if i == words.len() && !node.values.is_empty() {
                Self::end(ends, index);
            }
            for &child in &node.order {
                self.collect(child, words, i, ends);
            }
            if i < words.len() {
                self.collect(index, words, i + 1, ends);
            }
            return;
        }
        if i >= words.len() {
            return;
        }
        if !node.wildcard() && node.word != words[i] {
            return;
        }
        let next = i + 1;
        if next == words.len() {
            // A pattern ends here if anything was registered on this node,
            // independently of whether longer patterns branch off it.
            if !node.values.is_empty() {
                Self::end(ends, index);
            }
            if let Some(&hash) = node.children.get("#") {
                self.collect(hash, words, next, ends);
            }
            return;
        }
        for &child in &node.order {
            self.collect(child, words, next, ends);
        }
    }
}

/// Whether one pattern matches one topic.
pub fn topic_matches(pattern: &str, topic: &str) -> bool {
    let mut t = Trie::new();
    t.add(pattern, ());
    !t.matches(topic).is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(patterns: &[&str], topic: &str) -> Vec<String> {
        let mut t = Trie::new();
        for p in patterns {
            t.add(p, p.to_string());
        }
        t.matches(topic).into_iter().cloned().collect()
    }

    #[test]
    fn exact_and_star() {
        assert_eq!(m(&["a.b.c"], "a.b.c"), ["a.b.c"]);
        assert!(m(&["a.b.c"], "a.b").is_empty());
        assert!(m(&["a.b"], "a.b.c").is_empty());
        assert_eq!(m(&["a.*.c"], "a.b.c"), ["a.*.c"]);
        assert!(m(&["a.*"], "a").is_empty());
        assert!(m(&["a.*"], "a.b.c").is_empty());
        assert_eq!(m(&["*"], "a"), ["*"]);
    }

    #[test]
    fn hash_is_zero_or_more_words() {
        assert_eq!(m(&["#"], "a.b.c"), ["#"]);
        assert_eq!(m(&["a.#"], "a"), ["a.#"]);
        assert_eq!(m(&["a.#"], "a.b.c"), ["a.#"]);
        assert_eq!(m(&["#.c"], "a.b.c"), ["#.c"]);
        assert_eq!(m(&["#.c"], "c"), ["#.c"]);
        assert_eq!(m(&["a.#.c"], "a.c"), ["a.#.c"]);
        assert_eq!(m(&["a.#.c"], "a.x.y.c"), ["a.#.c"]);
        assert!(m(&["a.#.c"], "a.x.y").is_empty());
        assert_eq!(m(&["a.*.#"], "a.b"), ["a.*.#"]);
        assert!(m(&["a.*.#"], "a").is_empty());
    }

    #[test]
    fn every_matching_value_once_in_order() {
        assert_eq!(m(&["a.b", "a.*", "#", "x.y"], "a.b"), ["a.b", "a.*", "#"]);
        let mut t = Trie::new();
        t.add("a.#", 1);
        t.add("a.#", 2);
        assert_eq!(t.matches("a.b.c"), [&1, &2]);
    }

    #[test]
    fn a_prefix_pattern_matches_even_with_longer_siblings() {
        assert_eq!(m(&["a.b", "a.b.c"], "a.b"), ["a.b"]);
        assert_eq!(m(&["a.b", "a.b.c"], "a.b.c"), ["a.b.c"]);
    }

    #[test]
    fn single_patterns() {
        assert!(topic_matches("REQUEST.Calc.*", "REQUEST.Calc.add"));
        assert!(!topic_matches("REQUEST.Calc.*", "REQUEST.Calc.inst.add"));
    }
}
