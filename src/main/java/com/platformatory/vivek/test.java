package com.platformatory.vivek;

import java.util.UUID;

public class test {
    public static void main(String[] args) {

        String key = "vivek";
        String value = "123";

        // Generate a UUID based on the key and value
        UUID uuid = UUID.nameUUIDFromBytes((key + value).getBytes());
        System.out.println(uuid);
        System.out.println("hhhh");

        // Get the UUID as a string
        String uuidString = uuid.toString();

        // Print the generated UUID
        System.out.println(uuidString);

        long currentTimeMillis = System.currentTimeMillis();
        String sourceTimeAsValue = String.valueOf(currentTimeMillis);
        System.out.println(sourceTimeAsValue);

    }
}
/*
7d077f71-6c9a-30f5-a604-56534922464f
7d077f71-6c9a-30f5-a604-56534922464f
1dc36fe7-2f27-3b88-9d36-4dc4d4c7ffd8
7d077f71-6c9a-30f5-a604-56534922464f*/
