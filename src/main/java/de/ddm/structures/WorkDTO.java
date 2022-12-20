package de.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.File;
import java.util.List;

@Getter
@AllArgsConstructor
public class WorkDTO {

    private final File firstFile;
    private final String firstHeader;
    private final List<String> firstAttributes;

    private final File secondFile;
    private final String secondHeader;
    private final List<String> secondAttributes;

}
