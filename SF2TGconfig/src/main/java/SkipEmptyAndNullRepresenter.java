import org.yaml.snakeyaml.nodes.*;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.representer.Representer;

public class SkipEmptyAndNullRepresenter extends Representer{

    @Override
    protected NodeTuple representJavaBeanProperty(Object javaBean, Property property,
                                                  Object propertyValue, Tag customTag) {
        NodeTuple tuple = super.representJavaBeanProperty(javaBean, property, propertyValue,
                customTag);
        NodeTuple result = tuple;
        if (valueIsNull(tuple)
                || isAEmptyCollection(tuple)) {
            result=null;
        }

        return result;
    }

    public boolean valueIsNull(NodeTuple tuple) {
        Node valueNode = tuple.getValueNode();
        return Tag.NULL.equals(valueNode.getTag());
    }

    public boolean isAEmptyCollection(NodeTuple tuple) {
        return isASeqEmptyCollection(tuple)
                || isAMapEmptyCollection(tuple);
    }

    public boolean isASeqEmptyCollection(NodeTuple tuple) {
        boolean result = false;
        Node valueNode = tuple.getValueNode();
        if (isASeqCollection(tuple)) {
            SequenceNode seq = (SequenceNode) valueNode;
            result=seq.getValue().isEmpty();
        }
        return result;
    }

    public boolean isASeqCollection(NodeTuple tuple) {
        Node valueNode = tuple.getValueNode();
        return isACollection(tuple)
                && Tag.SEQ.equals(valueNode.getTag());
    }

    public boolean isACollection(NodeTuple tuple) {
        Node valueNode = tuple.getValueNode();
        return valueNode instanceof CollectionNode;
    }

    public boolean isAMapEmptyCollection(NodeTuple tuple) {
        boolean result = false;
        Node valueNode = tuple.getValueNode();
        if (isAMapCollection(tuple)) {
            MappingNode seq = (MappingNode) valueNode;
            result=seq.getValue().isEmpty();
        }
        return result;
    }

    public boolean isAMapCollection(NodeTuple tuple) {
        Node valueNode = tuple.getValueNode();
        return isACollection(tuple)
                && Tag.MAP.equals(valueNode.getTag());
    }
}
