package org.apache.tika.eval.navigator;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.tika.io.IOUtils;


@Path("downloadFile")
public class FileDownloader {

    private final File srcRoot;
    public FileDownloader(File srcRoot) {
        this.srcRoot = srcRoot;
    }

    @GET
    @Path("/{FILENAME}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response FileDownloader(@PathParam("FILENAME") final String fileName) {

        if (fileName.contains("..") || fileName.contains("`"))
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        final File file = new File(srcRoot, fileName);

        if(file == null || !file.exists())
            throw new WebApplicationException(javax.ws.rs.core.Response.Status.NOT_FOUND);

        StreamingOutput stream = new StreamingOutput() {
            @Override
            public void write(OutputStream output) throws IOException {
                InputStream is = null;
                try {
                    is = new FileInputStream(file);
                    IOUtils.copy(is, output);
                } catch (Exception e) {
                    //TODO: log
                } finally {
                    if (is != null) {
                        is.close();
                    }
                }
            }
        };

        return Response.ok(stream, "text/html").header(
                "Content-Disposition", "attachment; filename=" + file.getName()).build();

    }

}
