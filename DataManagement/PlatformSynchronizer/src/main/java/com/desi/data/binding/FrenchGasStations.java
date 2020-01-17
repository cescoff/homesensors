package com.desi.data.binding;

import com.desi.data.utils.DistanceUtils;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import org.javatuples.Triplet;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement(name = "pdv_liste")
public class FrenchGasStations {

    @XmlElement(name =  "pdv")
    private List<GasStation> stations = Lists.newArrayList();

    public List<GasStation> getStations() {
        return stations;
    }
}
