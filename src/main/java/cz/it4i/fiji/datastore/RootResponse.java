package cz.it4i.fiji.datastore;

import cz.it4i.fiji.datastore.register_service.OperationMode;
import lombok.Builder;
import lombok.Getter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.StringWriter;
import java.util.List;

@Getter
@XmlRootElement
@Builder
public class RootResponse {
    @XmlElement
    final String uuid;

    @XmlElement
    private final int version;

    @XmlElement
    private final OperationMode mode;

    @XmlElement
    private final List<int[]> resolutionLevels;

    @XmlElement
    private final Long serverTimeout;

    public String toXml()
    {
        try {
            JAXBContext context = JAXBContext.newInstance(RootResponse.class);
            Marshaller marshaller = context.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

            StringWriter writer = new StringWriter();
            marshaller.marshal(this, writer);
            return writer.toString();
        } catch (JAXBException e) {
            throw new RuntimeException("Error converting to XML", e);
        }
    }
}
