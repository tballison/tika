package org.apache.tika.eval.navigator;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;

@Path("view")
public class JSONViewer {
    private final File targetDocumentRoot;

    public JSONViewer(File targetDocumentRoot) {
        this.targetDocumentRoot = targetDocumentRoot;
    }

    @GET
    @Path("/{FILENAME}")
    @Produces(MediaType.TEXT_HTML)
    public List<Metadata> getMetadata(@PathParam("FILENAME") final String fileName) {

        if (fileName.contains("..") || fileName.contains("`"))
            throw new WebApplicationException(Response.Status.BAD_REQUEST);

        File file = new File(targetDocumentRoot, fileName);

        if(file == null || !file.exists())
            throw new WebApplicationException(javax.ws.rs.core.Response.Status.NOT_FOUND);

        Reader reader;
        try {
            reader = new InputStreamReader(new FileInputStream(file));
        } catch (IOException e) {
            throw new WebApplicationException(javax.ws.rs.core.Response.Status.NOT_FOUND);
        }

        List<Metadata> metadata = null;
        try {
            metadata = JsonMetadataList.fromJson(reader);
        } catch (TikaException e) {
            e.printStackTrace();
        }
        return metadata;
    }
}
