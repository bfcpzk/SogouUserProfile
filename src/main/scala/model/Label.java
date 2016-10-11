package model;

/**
 * Created by zhaokangpan on 16/10/11.
 */
public class Label {

    private String id;
    private Age age;
    private Gender gender;
    private Education education;

    public Label(){
        this.age = new Age();
        this.gender = new Gender();
        this.education = new Education();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Gender getGender() {
        return gender;
    }

    public void setGender(Gender gender) {
        this.gender = gender;
    }

    public Age getAge() {
        return age;
    }

    public void setAge(Age age) {
        this.age = age;
    }

    public Education getEducation() {
        return education;
    }

    public void setEducation(Education education) {
        this.education = education;
    }
}
