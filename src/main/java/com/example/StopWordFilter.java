package com.example;
import java.util.Set;
import org.apache.crunch.FilterFn;
import com.google.common.collect.ImmutableSet;

public class StopWordFilter extends FilterFn<String> {
    private static final Set<String> STOP_WORDS = ImmutableSet.copyOf(new String[] {
        "a", "and", "are", "as", "at", "be", "but", "by",
        "for", "if", "in", "into", "is", "it",
        "no", "not", "of", "on", "or", "s", "such",
        "t", "that", "the", "their", "then", "there", "these",
        "they", "this", "to", "was", "will", "with"
    });

    @Override
    public boolean accept(String word){ return !STOP_WORDS.contains(word); }
}
