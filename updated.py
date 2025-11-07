def find_rule_matches_detailed(
    chunk_df: pd.DataFrame,
    rules: List[Dict[str, Any]],
    text_col: str = "Text_Chunk"
) -> pd.DataFrame:
    """
    Apply all rules to each chunk and record which rules were checked,
    which triggered, and why not triggered (justification).
    """
    results: List[Dict[str, Any]] = []

    for _, row in chunk_df.iterrows():
        text = str(row.get(text_col, "") or "")
        if not text:
            continue

        lower_text = text.lower()

        triggered_rules = []
        checked_rules = []
        justification = []

        for rule in rules:
            rule_id = rule.get("id")
            theme = rule.get("theme")
            sub_theme = rule.get("sub_theme")
            severity = rule.get("severity")

            pos_patterns = [p.lower() for p in rule.get("positive_patterns", [])]
            neg_patterns = [n.lower() for n in rule.get("negative_patterns", [])]

            checked_rules.append(rule_id)

            # Match check
            matched_pattern = None
            for p in pos_patterns:
                if p and p in lower_text:
                    matched_pattern = p
                    break

            # No positive pattern matched
            if not matched_pattern:
                justification.append(f"{rule_id}: No positive pattern match")
                continue

            # Filter false positives
            if any(n and n in lower_text for n in neg_patterns):
                justification.append(f"{rule_id}: Negative pattern triggered → ignored")
                continue

            # If reached here → valid trigger
            triggered_rules.append(f"{rule_id} ({matched_pattern})")

        # --- Build row result
        result: Dict[str, Any] = {
            "Message_ID": row.get("Message_ID"),
            "Chunk_ID": row.get("Chunk_ID"),
            "Text_Chunk": text,
            "Rules_Checked": ", ".join(checked_rules),
            "Rules_Triggered": ", ".join(triggered_rules) if triggered_rules else "None",
        }

        if triggered_rules:
            result["Review_Justification"] = (
                f"Triggered {len(triggered_rules)} rule(s): {', '.join(triggered_rules)}"
            )
        else:
            result["Review_Justification"] = (
                f"All {len(checked_rules)} rules checked — "
                "no positive patterns or red flags found."
            )

        results.append(result)

    return pd.DataFrame(results)


def main():
    print("🔍 Loading rules from", BIBLE_FILE)
    rules = load_rules(BIBLE_FILE)
    print(f"   Loaded {len(rules)} rules.")

    print("📥 Loading input Excel:", INPUT_EXCEL)
    df = pd.read_excel(INPUT_EXCEL)
    print(f"   Loaded {len(df)} messages.")

    print("✂️  Chunking text into smaller units...")
    chunk_df = create_chunked_dataframe(df)
    print(f"   Created {len(chunk_df)} chunks.")

    print("🧮 Applying rules with detailed traceability...")
    detailed_df = find_rule_matches_detailed(chunk_df, rules)
    print(f"   Processed {len(detailed_df)} chunks with detailed results.")

    print("📤 Saving detailed output to", OUTPUT_EXCEL)
    detailed_df.to_excel(OUTPUT_EXCEL, index=False)
    print("✅ Done. Review", OUTPUT_EXCEL, "for rule-level justifications.")
