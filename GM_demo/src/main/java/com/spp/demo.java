package com.spp;


public class demo {
    public static void main(String[] args) {
        int num = 777321;

        int reverse = reverse(num);

        System.out.println(reverse);
        return ;
    }


    static int reverse(int x) {
        int op = x>0?1:-1;
        char tmp;

        char[] chars = String.valueOf(Math.abs(x)).toCharArray();
        int length = chars.length;
        for (int i = 0; i < length; i++) {
            tmp=chars[i];
            chars[i] = chars[length-1+i];
            chars[length-1+i]= tmp;
        }

        return Integer.parseInt(String.valueOf(chars)) * op;

    }
}
