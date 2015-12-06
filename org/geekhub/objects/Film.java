package org.geekhub.objects;

import java.util.Date;

public class Film extends Entity {

    private String name;
    private String director;
    private String actor;
    private Integer oscars;
    private Date releaseDate;

    public Film(String name, String director, String actor, Integer oscars, Date releaseDate) {
        this.name = name;
        this.director = director;
        this.actor = actor;
        this.oscars = oscars;
        this.releaseDate = releaseDate;
    }

    public Film() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDirector() {
        return director;
    }

    public void setDirector(String director) {
        this.director = director;
    }

    public String getActor() {
        return actor;
    }

    public void setActor(String actor) {
        this.actor = actor;
    }

    public Integer getOscars() {
        return oscars;
    }

    public void setOscars(Integer oscars) {
        this.oscars = oscars;
    }

    public Date getReleaseDate() {
        return releaseDate;
    }

    public void setReleaseDate(Date releaseDate) {
        this.releaseDate = releaseDate;
    }

    @Override
    public String toString() {
        return name + ", " + actor;
    }
}
