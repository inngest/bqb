package bqb

import (
	"regexp"
	"strings"
)

func extractParamNames(input string) []string {
	r := regexp.MustCompile(`@([A-Za-z0-9_]+)`)
	var names []string
	for _, match := range r.FindAllStringSubmatch(input, -1) {
		if len(match) > 1 {
			names = append(names, match[1])
		}
	}

	return names
}

func indent(input string) string {
	lines := strings.Split(input, "\n")
	indentedLines := make([]string, len(lines))

	for i, line := range lines {
		indentedLines[i] = "  " + line
	}

	return strings.Join(indentedLines, "\n")
}

func sanitizeColumnName(input string) string {
	r := regexp.MustCompile("^[A-Za-z0-9_]+$")
	if !r.MatchString(input) {
		return ""
	}

	return input
}

func sanitizeTableName(input string) string {
	r := regexp.MustCompile("^[A-Za-z0-9-_.]+$")
	if !r.MatchString(input) {
		return ""
	}

	return input
}
