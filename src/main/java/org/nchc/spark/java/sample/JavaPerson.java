package org.nchc.spark.java.sample;

import java.io.Serializable;

public class JavaPerson implements Serializable {


    int id;
    String studentName;
    String phone;
    String email;

    public JavaPerson() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getStudentName() {
        return studentName;
    }

    public void setStudentName(String studentName) {
        this.studentName = studentName;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public JavaPerson(int id, String studentName, String phone, String email) {
        this.id = id;
        this.studentName = studentName;
        this.phone = phone;
        this.email = email;

    }

    @Override
    public String toString() {
        return "JavaPerson{" +
                "id=" + id +
                ", studentName='" + studentName + '\'' +
                ", phone='" + phone + '\'' +
                ", email='" + email + '\'' +
                '}';
    }
}
