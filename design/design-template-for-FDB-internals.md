# Design Template for FDB Internals 

This template provides a guide for capturing and discussing the design and implementation of FDB internals. It provides a skeleton for documenting existing FDB components and mechanisms and for proposing new FDB internal features. 

The design document of FDB internals serves for two goals: 

1. Education: Provide enough information for general FDB developers to better understand how a FDB internal component is designed and implemented;
2. Consensus on the design: Present enough details for newly proposed FDB internal component such that the related parties (including the feature’s contributor and committers who will review the code) can agree on the design.


Note that (1) not every section in the template has to be included in a design doc. The section that does not apply to a FDB internal component should be removed from its design doc. (2) the text in *italics *should be replaced with your own words.. 

Some examples of this template are as follows:

* [Data Distribution Internals](https://github.com/apple/foundationdb/blob/master/design/data-distributor-internals.md), which describes the data structure, mechanisms and operations in FDB’s data distribution feature;
* [FDB Recovery Internals](https://github.com/apple/foundationdb/blob/master/design/recovery-internals.md), which describes each step (i.e., operations) FDB recovery takes.

# Introduction

*Provide a brief description of the feature. Describe the problem solved by the feature or why FDB needs the feature.*
*A few sentences or one to two short paragraphs should be enough.*

# Background

*This section should include any information and context that help describe and discuss the feature. If some FDB-related knowledge is required for readers to understand the design document, it should be described here. If there exists a document to explain the knowledge, it is recommended to write a brief summary of the knowledge and provide the document’s link.*

# Terminology

*If there is any terminology that the reader may not be familiar with already, or any terminology that you are introducing as part of this feature, it should be included here, preferably as a bulleted list of the form.*

* ***Term** - Description of the term*

# Assumptions

*This section should describe any assumption that the feature is making or relying on. For example, assumptions on failures, networks and request patterns.*

# Design options

*If you are proposing a new feature and** there are multiple design approaches that were considered coming to this design, this is a good place to discuss them.  If you are in the process of trying to pick a particular design option, it would be a good idea to fill this section out, stop, and discuss it with others before proceeding with the remaining sections with the chosen option.*

*If there exists an alternative approach to implement the existing feature, it is still worthwhile to discuss the alternatives here.*

# Components and data structure

*List key components and data structures that are needed to realize the feature.*

# Mechanisms

*Define primitives the feature uses to realize its operations, which are used to solve the problem defined in the introduction.*

# Operations

*Explain the steps the feature performs to achieve its goal.*

# Compatibility

*If this feature needs special care when database administrator upgrades or downgrades FDB, describe what part of the feature affects the compatibility, *