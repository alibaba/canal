package com.alibaba.otter.canal.client.adapter.config.common;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;

import org.springframework.core.io.*;
import org.springframework.core.io.ByteArrayResource;

/**
 * Interface for a resource descriptor that abstracts from the actual type of
 * underlying resource, such as a file or class path resource.
 * <p>
 * An InputStream can be opened for every resource if it exists in physical
 * form, but a URL or File handle can just be returned for certain resources.
 * The actual behavior is implementation-specific.
 *
 * @author Juergen Hoeller
 * @since 28.12.2003
 * @see #getInputStream()
 * @see #getURL()
 * @see #getURI()
 * @see #getFile()
 * @see WritableResource
 * @see ContextResource
 * @see UrlResource
 * @see ClassPathResource
 * @see FileSystemResource
 * @see PathResource
 * @see ByteArrayResource
 * @see InputStreamResource
 */
public interface Resource extends InputStreamSource {

    boolean exists();

    boolean isReadable();

    boolean isOpen();

    URL getURL() throws IOException;

    URI getURI() throws IOException;

    File getFile() throws IOException;

    long contentLength() throws IOException;

    long lastModified() throws IOException;

    org.springframework.core.io.Resource createRelative(String var1) throws IOException;

    String getFilename();

    String getDescription();
}
