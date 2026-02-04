//! Glob Pattern Matching Module
//!
//! Provides Redis-compatible glob pattern matching for key scanning commands.
//! Supports the following patterns:
//! - `*` matches any sequence of characters (including empty)
//! - `?` matches any single character
//! - `[abc]` matches any character in the brackets
//! - `[a-z]` matches any character in the range
//! - `[^abc]` or `[!abc]` matches any character NOT in the brackets
//! - `\` escapes the next character (use `\\` for literal backslash)

/// A compiled glob pattern for efficient matching
#[derive(Debug, Clone)]
pub struct Pattern {
    /// Original pattern string for debugging
    #[allow(dead_code)]
    original: Vec<u8>,
    /// Compiled pattern tokens
    tokens: Vec<Token>,
}

#[derive(Debug, Clone)]
enum Token {
    /// Match any sequence of characters (including empty)
    Star,
    /// Match any single character
    Question,
    /// Match a literal byte
    Literal(u8),
    /// Match any character in the set
    CharClass { chars: Vec<u8>, negated: bool },
}

impl Pattern {
    /// Compile a glob pattern
    pub fn new(pattern: &[u8]) -> Self {
        let tokens = Self::compile(pattern);
        Self {
            original: pattern.to_vec(),
            tokens,
        }
    }

    /// Compile pattern into tokens
    fn compile(pattern: &[u8]) -> Vec<Token> {
        let mut tokens = Vec::new();
        let mut i = 0;

        while i < pattern.len() {
            match pattern[i] {
                b'*' => {
                    // Collapse consecutive stars into one
                    if !matches!(tokens.last(), Some(Token::Star)) {
                        tokens.push(Token::Star);
                    }
                    i += 1;
                }
                b'?' => {
                    tokens.push(Token::Question);
                    i += 1;
                }
                b'[' => {
                    let (token, consumed) = Self::parse_char_class(&pattern[i..]);
                    tokens.push(token);
                    i += consumed;
                }
                b'\\' if i + 1 < pattern.len() => {
                    // Escape next character
                    tokens.push(Token::Literal(pattern[i + 1]));
                    i += 2;
                }
                b'\\' => {
                    // Trailing backslash - treat as literal backslash
                    tokens.push(Token::Literal(b'\\'));
                    i += 1;
                }
                c => {
                    tokens.push(Token::Literal(c));
                    i += 1;
                }
            }
        }

        tokens
    }

    /// Parse a character class [...]
    /// Returns (Token, bytes consumed)
    ///
    /// Edge cases:
    /// - Empty class `[]` will not match any character (empty chars vector)
    /// - Unclosed bracket `[abc` treats remaining chars as class members
    /// - Reversed ranges like `[z-a]` are treated as literal characters
    fn parse_char_class(pattern: &[u8]) -> (Token, usize) {
        debug_assert!(pattern[0] == b'[');

        let mut chars = Vec::new();
        let mut negated = false;
        let mut i = 1;

        // Check for negation
        if i < pattern.len() && (pattern[i] == b'^' || pattern[i] == b'!') {
            negated = true;
            i += 1;
        }

        // Parse characters until closing bracket
        while i < pattern.len() && pattern[i] != b']' {
            if i + 2 < pattern.len() && pattern[i + 1] == b'-' && pattern[i + 2] != b']' {
                // Range like a-z
                let start = pattern[i];
                let end = pattern[i + 2];
                if start <= end {
                    for c in start..=end {
                        chars.push(c);
                    }
                } else {
                    // Invalid/reversed range like [z-a] - treat as literal characters
                    chars.push(start);
                    chars.push(b'-');
                    chars.push(end);
                }
                i += 3;
            } else if pattern[i] == b'\\' && i + 1 < pattern.len() {
                // Escaped character
                chars.push(pattern[i + 1]);
                i += 2;
            } else {
                chars.push(pattern[i]);
                i += 1;
            }
        }

        // Skip closing bracket if present
        if i < pattern.len() && pattern[i] == b']' {
            i += 1;
        }

        (Token::CharClass { chars, negated }, i)
    }

    /// Check if a key matches this pattern
    pub fn matches(&self, key: &[u8]) -> bool {
        Self::match_tokens(&self.tokens, key)
    }

    /// Recursive matching with backtracking for star wildcards
    fn match_tokens(tokens: &[Token], input: &[u8]) -> bool {
        let mut ti = 0; // token index
        let mut ii = 0; // input index

        // For backtracking on star matches
        let mut star_ti: Option<usize> = None;
        let mut star_ii: Option<usize> = None;

        while ii < input.len() || ti < tokens.len() {
            if ti < tokens.len() {
                match &tokens[ti] {
                    Token::Star => {
                        // Record position for backtracking
                        star_ti = Some(ti);
                        star_ii = Some(ii);
                        ti += 1;
                        continue;
                    }
                    Token::Question if ii < input.len() => {
                        // Match any single character
                        ti += 1;
                        ii += 1;
                        continue;
                    }
                    Token::Literal(c) if ii < input.len() && input[ii] == *c => {
                        ti += 1;
                        ii += 1;
                        continue;
                    }
                    Token::CharClass { chars, negated } if ii < input.len() => {
                        // Empty character class should never match any character,
                        // even when negated (e.g., "[^]" or "[!]" should match nothing).
                        // This deviates from some regex semantics but matches Redis-style glob behavior.
                        if chars.is_empty() {
                            // Do not advance; fall through to backtracking logic
                        } else {
                            let matches_class = chars.contains(&input[ii]);
                            let should_match = if *negated {
                                !matches_class
                            } else {
                                matches_class
                            };
                            if should_match {
                                ti += 1;
                                ii += 1;
                                continue;
                            }
                        }
                    }
                    _ => {}
                }
            }

            // No match - try backtracking
            if let (Some(sti), Some(sii)) = (star_ti, star_ii) {
                // Consume one more character with the star
                ti = sti + 1;
                ii = sii + 1;
                star_ii = Some(sii + 1);

                if ii <= input.len() {
                    continue;
                }
            }

            // No match and no backtrack available
            return false;
        }

        true
    }

    /// Check if the pattern matches everything (is just "*")
    #[allow(dead_code)]
    pub fn matches_all(&self) -> bool {
        self.tokens.len() == 1 && matches!(self.tokens[0], Token::Star)
    }

    /// Get the original pattern bytes
    #[allow(dead_code)]
    pub fn as_bytes(&self) -> &[u8] {
        &self.original
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_star_wildcard() {
        let pattern = Pattern::new(b"*");
        assert!(pattern.matches(b""));
        assert!(pattern.matches(b"hello"));
        assert!(pattern.matches(b"world"));
        assert!(pattern.matches_all());
    }

    #[test]
    fn test_star_prefix() {
        let pattern = Pattern::new(b"hello*");
        assert!(pattern.matches(b"hello"));
        assert!(pattern.matches(b"helloworld"));
        assert!(pattern.matches(b"hello123"));
        assert!(!pattern.matches(b"hell"));
        assert!(!pattern.matches(b"world"));
    }

    #[test]
    fn test_star_suffix() {
        let pattern = Pattern::new(b"*world");
        assert!(pattern.matches(b"world"));
        assert!(pattern.matches(b"helloworld"));
        assert!(pattern.matches(b"123world"));
        assert!(!pattern.matches(b"worldx"));
    }

    #[test]
    fn test_star_middle() {
        let pattern = Pattern::new(b"h*d");
        assert!(pattern.matches(b"hd"));
        assert!(pattern.matches(b"helloworld"));
        assert!(pattern.matches(b"had"));
        assert!(!pattern.matches(b"hello"));
    }

    #[test]
    fn test_question_wildcard() {
        let pattern = Pattern::new(b"h?llo");
        assert!(pattern.matches(b"hello"));
        assert!(pattern.matches(b"hallo"));
        assert!(pattern.matches(b"hxllo"));
        assert!(!pattern.matches(b"hllo"));
        assert!(!pattern.matches(b"heello"));
    }

    #[test]
    fn test_character_class() {
        let pattern = Pattern::new(b"h[ae]llo");
        assert!(pattern.matches(b"hello"));
        assert!(pattern.matches(b"hallo"));
        assert!(!pattern.matches(b"hillo"));
        assert!(!pattern.matches(b"hollo"));
    }

    #[test]
    fn test_character_range() {
        let pattern = Pattern::new(b"key[0-9]");
        assert!(pattern.matches(b"key0"));
        assert!(pattern.matches(b"key5"));
        assert!(pattern.matches(b"key9"));
        assert!(!pattern.matches(b"keya"));
        assert!(!pattern.matches(b"key10"));
    }

    #[test]
    fn test_negated_class() {
        let pattern = Pattern::new(b"h[^aeiou]llo");
        assert!(!pattern.matches(b"hello"));
        assert!(!pattern.matches(b"hallo"));
        assert!(pattern.matches(b"hxllo"));
        assert!(pattern.matches(b"h1llo"));
    }

    #[test]
    fn test_negated_class_bang() {
        let pattern = Pattern::new(b"h[!aeiou]llo");
        assert!(!pattern.matches(b"hello"));
        assert!(pattern.matches(b"hxllo"));
    }

    #[test]
    fn test_escaped_chars() {
        let pattern = Pattern::new(b"hello\\*world");
        assert!(pattern.matches(b"hello*world"));
        assert!(!pattern.matches(b"helloworld"));
        assert!(!pattern.matches(b"helloXworld"));

        let pattern = Pattern::new(b"hello\\?world");
        assert!(pattern.matches(b"hello?world"));
        assert!(!pattern.matches(b"helloXworld"));
    }

    #[test]
    fn test_complex_patterns() {
        let pattern = Pattern::new(b"user:*:profile");
        assert!(pattern.matches(b"user:123:profile"));
        assert!(pattern.matches(b"user:alice:profile"));
        assert!(!pattern.matches(b"user:123:settings"));

        let pattern = Pattern::new(b"*:*:*");
        assert!(pattern.matches(b"a:b:c"));
        assert!(pattern.matches(b"user:123:profile"));
        assert!(!pattern.matches(b"a:b"));

        let pattern = Pattern::new(b"key???");
        assert!(pattern.matches(b"key123"));
        assert!(pattern.matches(b"keyabc"));
        assert!(!pattern.matches(b"key12"));
        assert!(!pattern.matches(b"key1234"));
    }

    #[test]
    fn test_empty_pattern() {
        let pattern = Pattern::new(b"");
        assert!(pattern.matches(b""));
        assert!(!pattern.matches(b"hello"));
    }

    #[test]
    fn test_literal_only() {
        let pattern = Pattern::new(b"hello");
        assert!(pattern.matches(b"hello"));
        assert!(!pattern.matches(b"hello!"));
        assert!(!pattern.matches(b"hell"));
        assert!(!pattern.matches(b"Hello"));
    }

    #[test]
    fn test_multiple_stars() {
        let pattern = Pattern::new(b"*a*b*");
        assert!(pattern.matches(b"ab"));
        assert!(pattern.matches(b"aabb"));
        assert!(pattern.matches(b"xaxbx"));
        assert!(pattern.matches(b"123a456b789"));
        assert!(!pattern.matches(b"ba"));
    }

    #[test]
    fn test_redis_patterns() {
        // Common Redis patterns
        let pattern = Pattern::new(b"session:*");
        assert!(pattern.matches(b"session:abc123"));
        assert!(pattern.matches(b"session:"));
        assert!(!pattern.matches(b"sessions:abc"));

        let pattern = Pattern::new(b"user:[0-9]*");
        assert!(pattern.matches(b"user:1"));
        assert!(pattern.matches(b"user:123456"));
        assert!(!pattern.matches(b"user:alice"));

        let pattern = Pattern::new(b"cache:*:v[0-9]");
        assert!(pattern.matches(b"cache:item:v1"));
        assert!(pattern.matches(b"cache:other:v9"));
        assert!(!pattern.matches(b"cache:item:v10"));
    }

    // ========== Edge Case Tests ==========

    #[test]
    fn test_unclosed_character_class() {
        // Unclosed bracket should still work, treating remaining chars as class members
        let pattern = Pattern::new(b"[abc");
        assert!(pattern.matches(b"a"));
        assert!(pattern.matches(b"b"));
        assert!(pattern.matches(b"c"));
        assert!(!pattern.matches(b"d"));
    }

    #[test]
    fn test_trailing_backslash() {
        // Trailing backslash should be treated as literal backslash
        let pattern = Pattern::new(b"test\\");
        assert!(pattern.matches(b"test\\"));
        assert!(!pattern.matches(b"test"));
    }

    #[test]
    fn test_reversed_range() {
        // Reversed range like [z-a] should be treated as literal chars 'z', '-', 'a'
        let pattern = Pattern::new(b"[z-a]");
        assert!(pattern.matches(b"z"));
        assert!(pattern.matches(b"-"));
        assert!(pattern.matches(b"a"));
        assert!(!pattern.matches(b"m")); // Not in range since range is invalid
    }

    #[test]
    fn test_empty_character_class() {
        // Empty character class [] should match nothing
        let pattern = Pattern::new(b"[]");
        assert!(!pattern.matches(b"a"));
        assert!(!pattern.matches(b""));
    }

    #[test]
    fn test_backslash_in_class() {
        // Escaped character in class
        let pattern = Pattern::new(b"[\\]]");
        assert!(pattern.matches(b"]"));
    }

    #[test]
    fn test_dash_before_closing_bracket() {
        // Pattern [a-] should match 'a' and '-' as literal characters, not create a range
        // The closing bracket ']' is not part of the character class
        let pattern = Pattern::new(b"[a-]");
        assert!(pattern.matches(b"a"));
        assert!(pattern.matches(b"-"));
        assert!(!pattern.matches(b"]")); // Closing bracket is not part of the class
        assert!(!pattern.matches(b"b")); // Should not match anything else
    }
}
