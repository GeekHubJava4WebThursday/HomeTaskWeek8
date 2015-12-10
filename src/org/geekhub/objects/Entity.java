package org.geekhub.objects;

/**
 * Base class for all objects that could be stored with any {@link org.geekhub.storage.Storage} implementation.
 */
public abstract class Entity {

    /**
     * Identifier of the entity. Should be updated only by {@link org.geekhub.storage.Storage} implementation.
     */
    private Integer id;

    /**
     * Determines is object new or not based on its id.
     * @return true if object not yet saved, false otherwise.
     */
    public boolean isNew() {
        return getId() == null;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }
}
