package com.google.cloud.teleport.v2.templates.models;

public class PreparedStatementValueObject<T> {
    private String key;
    private T value;

    public PreparedStatementValueObject(String key, T value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    // Override toString() for better readability when printing objects
    @Override
    public String toString() {
        return "PreparedStatementValueObject{" +
                "key='" + key + '\'' +
                ", value=" + value +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        PreparedStatementValueObject<?> that = (PreparedStatementValueObject<?>) obj;
        return key.equals(that.key) && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return 31 * key.hashCode() + (value != null ? value.hashCode() : 0);
    }
}

