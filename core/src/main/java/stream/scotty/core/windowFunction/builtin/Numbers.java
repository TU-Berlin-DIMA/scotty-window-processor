package stream.scotty.core.windowFunction.builtin;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Arithmetic on boxed numbers that keeps the type of the operands, e.g. Integer + Integer = Integer.
 */
final class Numbers {

    private Numbers() {
    }

    @SuppressWarnings("unchecked")
    static <T extends Number> T add(T a, T b) {
        checkSameType(a, b);
        if (a instanceof Integer) {
            return (T) Integer.valueOf(a.intValue() + b.intValue());
        } else if (a instanceof Long) {
            return (T) Long.valueOf(a.longValue() + b.longValue());
        } else if (a instanceof Double) {
            return (T) Double.valueOf(a.doubleValue() + b.doubleValue());
        } else if (a instanceof Float) {
            return (T) Float.valueOf(a.floatValue() + b.floatValue());
        } else if (a instanceof BigInteger) {
            return (T) ((BigInteger) a).add((BigInteger) b);
        } else if (a instanceof BigDecimal) {
            return (T) ((BigDecimal) a).add((BigDecimal) b);
        }
        throw unsupported(a);
    }

    @SuppressWarnings("unchecked")
    static <T extends Number> T subtract(T a, T b) {
        checkSameType(a, b);
        if (a instanceof Integer) {
            return (T) Integer.valueOf(a.intValue() - b.intValue());
        } else if (a instanceof Long) {
            return (T) Long.valueOf(a.longValue() - b.longValue());
        } else if (a instanceof Double) {
            return (T) Double.valueOf(a.doubleValue() - b.doubleValue());
        } else if (a instanceof Float) {
            return (T) Float.valueOf(a.floatValue() - b.floatValue());
        } else if (a instanceof BigInteger) {
            return (T) ((BigInteger) a).subtract((BigInteger) b);
        } else if (a instanceof BigDecimal) {
            return (T) ((BigDecimal) a).subtract((BigDecimal) b);
        }
        throw unsupported(a);
    }

    private static void checkSameType(Number a, Number b) {
        if (a.getClass() != b.getClass()) {
            throw new IllegalArgumentException("Cannot combine values of different types: "
                    + a.getClass().getName() + " and " + b.getClass().getName());
        }
    }

    private static IllegalArgumentException unsupported(Number a) {
        return new IllegalArgumentException("Unsupported number type: " + a.getClass().getName()
                + ". Supported types are Integer, Long, Float, Double, BigInteger and BigDecimal.");
    }
}
