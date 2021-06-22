package de.tub.dima.scotty.demo.spark.windowFunctions;

import java.util.Map;
import java.util.TreeMap;

public class QuantileTreeMap implements java.io.Serializable {

    private TreeMap<Integer, Integer> values = new TreeMap<>();
    private int quantile;
    private int lowercount = 0;
    private int uppercount = 0;
    private double pct;
    public static int counter;

    public QuantileTreeMap(Integer initialValue, double pct) {
        values.put(initialValue, 1);
        quantile = initialValue;
        this.pct = pct;
    }

    private QuantileTreeMap(int quantile,
                            int lowercount,
                            int uppercount,
                            double pct,
                            TreeMap<Integer, Integer> values) {
        this.quantile = quantile;
        this.lowercount = lowercount;
        this.uppercount = uppercount;
        this.pct = pct;
        this.values = values;
    }

    public QuantileTreeMap addValue(Integer value) {
        //counter++;
        values.compute(value, (k, v) -> (v == null) ? 1 : ++v);

        if (value < quantile) {
            lowercount++;
        } else if (value > quantile) {
            uppercount++;
        }
        updateQuantile();
        return this;
    }

    public void addValues(TreeMap<Integer, Integer> newValues) {


        TreeMap<Integer, Integer> outer = newValues;
        TreeMap<Integer, Integer> innter = values;

        counter = (innter.size() - outer.size());

        for (Map.Entry<Integer, Integer> entry : outer.entrySet()) {
            if (innter.containsKey(entry.getKey())) {
                innter.put(entry.getKey(), innter.get(entry.getKey()) + entry.getValue());
            } else {
                innter.put(entry.getKey(), entry.getValue());
            }
            if (entry.getKey() < quantile) {
                lowercount++;
            } else if (entry.getKey() > quantile) {
                uppercount++;
            }
        }
        values = innter;

        updateQuantile();
    }

    /**
     * Merges another quantile into this one.
     * Be aware that this method returns the same object.
     * This method will NOT clone the Quantile object for merging it.
     *
     * @param q the quantile to merge
     * @return this object: This method will NOT clone the Quantile object for merging it.
     */
    public QuantileTreeMap merge(QuantileTreeMap q) {

        addValues(q.getValues());
        return this;
    }

    public TreeMap<Integer, Integer> getValues() {
        return values;
    }

    public int getQuantile() {
        return quantile;
    }

    private void updateQuantile() {
        //counter++;
        int totalCount = lowercount + values.get(quantile) + uppercount;
        int currentCount = values.get(quantile);
        double border = ((double) totalCount * pct);

        //increase quantile
        while (uppercount > totalCount - Math.ceil(border)) {

            Map.Entry<Integer, Integer> tmp = values.higherEntry(quantile);
            lowercount += currentCount;
            uppercount -= tmp.getValue();
            currentCount = tmp.getValue();
            quantile = tmp.getKey();
        }

        //decrease quantile
        while (lowercount > (int) border) {
            Map.Entry<Integer, Integer> tmp = values.lowerEntry(quantile);
            uppercount += currentCount;
            lowercount -= tmp.getValue();
            currentCount = tmp.getValue();
            quantile = tmp.getKey();
        }
    }

    public QuantileTreeMap clone() {
        return new QuantileTreeMap(
                quantile,
                lowercount,
                uppercount,
                pct,
                (TreeMap<Integer, Integer>) values.clone()
        );
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final QuantileTreeMap quantile1 = (QuantileTreeMap) o;

        if (quantile != quantile1.quantile) return false;
        if (lowercount != quantile1.lowercount) return false;
        if (uppercount != quantile1.uppercount) return false;
        if (Double.compare(quantile1.pct, pct) != 0) return false;
        return values != null ? values.equals(quantile1.values) : quantile1.values == null;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = values != null ? values.hashCode() : 0;
        result = 31 * result + quantile;
        result = 31 * result + lowercount;
        result = 31 * result + uppercount;
        temp = Double.doubleToLongBits(pct);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
