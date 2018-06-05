package com.alibaba.canal.plumber.schema.schema;


import com.alibaba.canal.plumber.schema.schema.define.EmbedSchema;
import com.alibaba.canal.plumber.schema.schema.define.EventSchema;
import com.alibaba.canal.plumber.schema.schema.define.ExcludeSchema;
import com.alibaba.canal.plumber.schema.schema.define.IncludeSchema;
import java.util.Map;

/**
 * 一个完整的schema定义
 * @author dsqin
 * @date 2018/6/5
 */
public class Schemas {

    private EventSchema event;
    private IncludeSchema includes;
    private ExcludeSchema excludes;
    private Map<String, String> aliases;
    private EmbedSchema embed;

    public EventSchema getEvent()
    {
        return this.event;
    }

    public void setEvent(EventSchema event)
    {
        this.event = event;
    }

    public IncludeSchema getIncludes()
    {
        return this.includes;
    }

    public void setIncludes(IncludeSchema includes)
    {
        this.includes = includes;
    }

    public ExcludeSchema getExcludes()
    {
        return this.excludes;
    }

    public void setExcludes(ExcludeSchema excludes)
    {
        this.excludes = excludes;
    }

    public Map<String, String> getAliases()
    {
        return this.aliases;
    }

    public void setAliases(Map<String, String> aliases)
    {
        this.aliases = aliases;
    }

    public EmbedSchema getEmbed()
    {
        return this.embed;
    }

    public void setEmbed(EmbedSchema embed)
    {
        this.embed = embed;
    }
}
