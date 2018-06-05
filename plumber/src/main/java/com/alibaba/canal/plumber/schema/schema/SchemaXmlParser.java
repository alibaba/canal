package com.alibaba.canal.plumber.schema.schema;


import com.alibaba.canal.plumber.schema.schema.define.AliasSchema;
import com.alibaba.canal.plumber.schema.schema.define.EmbedSchema;
import com.alibaba.canal.plumber.schema.schema.define.EventSchema;
import com.alibaba.canal.plumber.schema.schema.define.ExcludeSchema;
import com.alibaba.canal.plumber.schema.schema.define.IncludeSchema;
import com.google.common.collect.Maps;
import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Map;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/**
 * schema xml文件解析类
 * @author dsqin
 * @date 2018/6/5
 */
public class SchemaXmlParser {

    private static final String XML_FILE_PATH = "mongo-transform.xml";

    public static Map<String, Schemas> parse()
    {
        SAXReader reader = new SAXReader();
        URL url = SchemaXmlParser.class.getClassLoader().getResource(XML_FILE_PATH);
        File file = new File(url.getFile());
        Document document = null;
        try
        {
            document = reader.read(file);
        }
        catch (DocumentException e)
        {
            return null;
        }
        Element root = document.getRootElement();

        List<Element> childElements = root.elements();
        if ((null == childElements) || (childElements.isEmpty())) {
            return null;
        }
        Map<String, Schemas> schemasMap = Maps.newHashMap();
        for (Element child : childElements)
        {
            EventSchema event = new EventSchema(child.attributes());
            if (event.isValid())
            {
                Schemas schemas = new Schemas();

                schemas.setEvent(event);

                Element includeElement = child.element(SchemaLabel.INCLUDES);
                if (null != includeElement)
                {
                    IncludeSchema include = new IncludeSchema(includeElement);

                    schemas.setIncludes(include);
                }
                Element excludeElement = child.element(SchemaLabel.EXCLUDES);
                if (null != excludeElement)
                {
                    ExcludeSchema exclude = new ExcludeSchema(excludeElement);

                    schemas.setExcludes(exclude);
                }
                Element aliasesRootElement = child.element(SchemaLabel.ALIASES);
                if (null != aliasesRootElement)
                {
                    List<Element> aliasesElement = aliasesRootElement.elements();

                    Map<String, String> aliasesMap = Maps.newHashMap();
                    if ((null != aliasesElement) && (!aliasesElement.isEmpty())) {
                        for (Element element : aliasesElement)
                        {
                            AliasSchema aliasSchema = new AliasSchema(element);
                            if ((null != aliasSchema) && (aliasSchema.isValid())) {
                                aliasSchema.add(aliasesMap);
                            }
                        }
                    }
                    schemas.setAliases(aliasesMap);
                }
                Element embedElement = child.element(SchemaLabel.EMBED);
                if (null != embedElement)
                {
                    EmbedSchema embedSchema = new EmbedSchema(embedElement);

                    schemas.setEmbed(embedSchema);
                }
                schemasMap.put(event.id, schemas);
            }
        }
        return schemasMap;
    }
}
