package com.tigergraph.spark_connector.utils;

import java.text.DecimalFormat;

public class ProgressUtil {

    private static ProgressUtil progressUtil = new ProgressUtil();

    private int barLen = 50;

    /**
     * The character used to display the progress bar
     */
    private char showChar = '*';

    private String title = "Write Data Progress";

    private DecimalFormat formater = new DecimalFormat("#.##%");

    public ProgressUtil(){

    }

    public ProgressUtil(int barLen, char showChar, String title) {
        this.barLen = barLen;
        this.showChar = showChar;
        this.title = title;
    }

    public static String drawLine(int value){
        return progressUtil.show(value);
    }

    public String show(int value) {
        if (value < 0 || value > 100) {
            return "";
        }

        // ratio
        float rate = (float) (value * 1.0 / 100);
        // rate * total length of progress bar = current length
        return draw(barLen, rate);
    }


    private String draw(int barLen, float rate) {
        int len = (int) (rate * barLen);

        StringBuilder sb = new StringBuilder();
        sb.append(title + ": ");

        for (int i = 0; i < len; i++) {
            sb.append(showChar);
        }
        for (int i = 0; i < barLen - len; i++) {
            sb.append(" ");
        }
        sb.append(" |" + format(rate));
        return sb.toString();
    }


    private String format(float num) {
        return formater.format(num);
    }
}
