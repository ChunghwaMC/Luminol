package me.earthme.luminol.utils;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public record Pair<T, T1>(T left, T1 right) {
    @Contract("_, _ -> new")
    public static <R, R1> @NotNull Pair<R, R1> of(R left, R1 right) {
        return new Pair<>(left, right);
    }
}
