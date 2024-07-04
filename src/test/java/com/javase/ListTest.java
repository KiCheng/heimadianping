package com.javase;


import net.sf.jsqlparser.statement.select.First;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ListTest {
    public static void main(String[] args) {
        Integer[] arr = new Integer[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        List<Integer> list1 = Arrays.asList(arr);
        int e = list1.get(5);  // 查
        list1.set(5, 111);  // 改
//        list1.add(11);  // 增
//        list1.remove(5);  // 删

        ArrayList<Integer> list2 = new ArrayList<>(Arrays.asList(arr));
        list2.add(11);
        System.out.println(list2);

        ArrayList<Integer> list3 = new ArrayList<>(arr.length);
        Collections.addAll(list3, arr);
        list3.add(12);
        System.out.println(list3);

        // 创建一个数组
        Integer[] arr2 = list3.stream().toArray(Integer[]::new);
        System.out.println(Arrays.toString(arr2));
    }
}
