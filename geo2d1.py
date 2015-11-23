#!/usr/bin/env python

# geo2d1.py - Tier 1 code to export geonetwork metadata into a DataONE GMN.
# The International Arctic Research Center (IARC) Data Archive uses the xml
# form of ISO 19115 metadata, ISO 19139. Not all researchers fill out
# metadata that validates, however, so this code xslt transforms the public
# ISO 19139 metadata records from IARC's geonetwork OAI-PMH endpoint into
# dublin core extended (dcx), and inserts the corresponding geonetwork page 
# under the 'source' element. It then uploads the validated dcx into IARC's 
# DataONE GMN as metadata, while the original ISO 19139 metadata is uploaded 
# as data (text/xml). A resource map relating the two completes the package.
# Each time this script runs, it checks to see if a package update is required:
# the current GMN ISO 19139 data object (xml) is compared with the downloaded
# OAI-PMH version, and if different, triggers the package update.

# takes an optional argument, the record number to start with  
# (default = 1), useful if the script needs to be restarted

# FORCE_UPDATE can also be set to True from False to force updates, useful in a 
# situation where the XSLT transform changes, etc.

# requires python version < 2.7.9 if the GMN has no/invalid site certificate.


# Copyright (C) 2015, University of Alaska Fairbanks
# International Arctic Research Center
# Author: James Long
#-------------------------------------------------------------------------------
# GEO2D1 BSD License
# 
# Redistribution and use in source and binary forms, with or without 
# modification, are permitted provided that the following conditions are met:
# 
#   # Redistributions of source code must retain the above copyright notice, 
#      this list of conditions and the following disclaimer.
#   # Redistributions in binary form must reproduce the above copyright notice,
#      this list of conditions and the following disclaimer in the documentation
#      and/or other materials provided with the distribution.
#   # Neither the name of the University of Alaska Fairbanks nor the names of 
#      its contributors may be used to endorse or promote products derived from 
#      this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE 
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


# stdlib
#import logging
import hashlib
import lxml.etree as et
import os
import StringIO

from datetime import datetime
from sys import argv
from time import sleep
from urllib2 import urlopen

# 3rd party
import pyxb

# DataONE
import d1_common.types.generated.dataoneTypes as dataoneTypes
import d1_common.const
import d1_client.data_package
import d1_client.mnclient
import d1_common.types.exceptions

GEO_URL  = 'http://climate.iarc.uaf.edu/geonetwork/srv/en/main.home/oaipmh'
GMN_URL  = 'https://trusty.iarc.uaf.edu/mn'
FORCE_UPDATE = False
CERTIFICATE_FOR_CREATE      = '/home/jlong/d1/keys/jl_cert.pem'
CERTIFICATE_FOR_CREATE_KEY  = '/home/jlong/d1/keys/jl_key.pem'
SYSMETA_RIGHTSHOLDER        = 'CN=jlong,O=International Arctic Research Center,ST=AK,C=US'

# Object format types. A complete list of valid formatIds
# can be found at https://cn.dataone.org/cn/v1/formats
DATA_FORMAT_ID = 'text/xml'
META_FORMAT_ID = 'http://ns.dataone.org/metadata/schema/onedcx/v1.0'
RMAP_FORMAT_ID = 'http://www.openarchives.org/ore/terms'

def main():
  #logging.basicConfig()
  #logging.getLogger('').setLevel(logging.DEBUG)
  
  if len(argv) > 1 and not argv[1].isdigit():
    print "the argument " + argv[1] + " is not composed of all digits, returning..."
    return
    
  if len(argv) > 1 and int(argv[1]) < 1:
    print "the argument " + argv[1] + " is less than 1, returning..."
    return

  # get the list of ISO 19139 identifiers (fileIDs)
  print "Downloading list of Identifiers from " + GEO_URL + "..."
  try:
    fo = urlopen(GEO_URL + "?verb=ListIdentifiers&metadataPrefix=iso19139")
  except:
    print "URL open failure for " + GEO_URL + ", halting (try running this script again)..."
    return

  try:
    xmlDoc = fo.read()
  except:
    print "file read failure at " + GEO_URL + ", halting (try running this script again)..."
    return
  else:
    root    = et.fromstring(xmlDoc)
    fileIDs = [ i.text for i in root.findall("./{http://www.openarchives.org/OAI/2.0/}ListIdentifiers/{http://www.openarchives.org/OAI/2.0/}header/{http://www.openarchives.org/OAI/2.0/}identifier") ]
    rt      = root.findall("./{http://www.openarchives.org/OAI/2.0/}ListIdentifiers/{http://www.openarchives.org/OAI/2.0/}resumptionToken")

  print "downloading..."

  if len(rt)==0:
    print "Error retrieving ListIdentifiers on " + GEO_URL + " (check the OAI-PMH server), exiting..."
    return

  while rt[0].text:
    sleep(0.2)
    try:
      fo = urlopen(GEO_URL + "?verb=ListIdentifiers&resumptionToken=" + rt[0].text)
    except:
      print "URL open failure for " + GEO_URL + ", halting (try running this script again)..."
      return

    try:
      xmlDoc  = fo.read()
    except:
      print "file read failure at " + GEO_URL + ", halting (try running this script again)..."
      return
    else:
      root    = et.fromstring(xmlDoc)
      fileIDs = fileIDs + [ i.text for i in root.findall("./{http://www.openarchives.org/OAI/2.0/}ListIdentifiers/{http://www.openarchives.org/OAI/2.0/}header/{http://www.openarchives.org/OAI/2.0/}identifier") ]
      print "downloading..."
      rt      = root.findall("./{http://www.openarchives.org/OAI/2.0/}ListIdentifiers/{http://www.openarchives.org/OAI/2.0/}resumptionToken")

  # uniq the list
  fileIDs = list(set(fileIDs))
  
  if len(argv) > 1 and int(argv[1]) > len(fileIDs):
    print "the argument " + argv[1] + " is larger than the number of records, " + str(len(fileIDs)) + ","
    print "returning..."
    return
  
  print "number of unique Identifiers = ", len(fileIDs)

  # xsl doc to xslt transform OAI-PMH ISO 19139 record to dcx
  # test this on the command line by saving it in file 'test.xsl', and running
  # $ xsltproc test.xsl <xml file to transform>
  xslDoc = et.XML('''\
<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0"
                xmlns:gco="http://www.isotc211.org/2005/gco"
                xmlns:gmd="http://www.isotc211.org/2005/gmd"
                xmlns:gml="http://www.opengis.net/gml">
  <xsl:output
    indent="yes"
    method="xml"
    version="1.0"
  />

  <xsl:template match="gmd:MD_Metadata">
    <xsl:value-of select="concat('', '&#10;')"/>
    <metadata xmlns="http://ns.dataone.org/metadata/schema/onedcx/v1.0"
         xmlns:dc="http://purl.org/dc/terms/"
         xmlns:dcterms="http://purl.org/dc/terms/"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://ns.dataone.org/metadata/schema/onedcx/v1.0 http://ns.dataone.org/metadata/schema/onedcx/v1.0/onedcx_v1.0.xsd">
      
      <simpleDc>
        <xsl:for-each select="gmd:fileIdentifier">
          <dc:identifier><xsl:value-of select="gco:CharacterString"/></dc:identifier>
          <dc:source>http://climate.iarc.uaf.edu/geonetwork/srv/en/main.home?uuid=<xsl:value-of select="gco:CharacterString"/></dc:source>
        </xsl:for-each>

        <!-- DataIdentification - - - - - - - - - - - - - - - - - - - - - -->
        <xsl:for-each select="gmd:identificationInfo/gmd:MD_DataIdentification">

          <xsl:for-each select="gmd:citation/gmd:CI_Citation">
            <xsl:for-each select="gmd:title/gco:CharacterString">
              <dc:title><xsl:value-of select="."/></dc:title>
            </xsl:for-each>

            <xsl:for-each select="gmd:citedResponsibleParty/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode/@codeListValue='originator']/gmd:organisationName/gco:CharacterString">
              <dc:creator><xsl:value-of select="."/></dc:creator>
            </xsl:for-each>

            <xsl:for-each select="gmd:citedResponsibleParty/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode/@codeListValue='publisher']/gmd:organisationName/gco:CharacterString">
              <dc:publisher><xsl:value-of select="."/></dc:publisher>
            </xsl:for-each>

            <xsl:for-each select="gmd:citedResponsibleParty/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode/@codeListValue='author']/gmd:organisationName/gco:CharacterString">
              <dc:contributor><xsl:value-of select="."/></dc:contributor>
            </xsl:for-each>
          </xsl:for-each>

          <!-- subject -->
          <xsl:for-each select="gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString">
            <dc:subject><xsl:value-of select="."/></dc:subject>
          </xsl:for-each>

          <!-- language -->
          <xsl:for-each select="gmd:language/gco:CharacterString">
            <dc:language><xsl:value-of select="."/></dc:language>
          </xsl:for-each>

        </xsl:for-each>

        <!-- Type - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -->
        <xsl:for-each select="gmd:hierarchyLevel/gmd:MD_ScopeCode/@codeListValue">
          <dc:type><xsl:value-of select="."/></dc:type>
        </xsl:for-each>

        <!-- Distribution - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -->
        <xsl:for-each select="gmd:distributionInfo/gmd:MD_Distribution">
          <xsl:for-each select="gmd:distributionFormat/gmd:MD_Format/gmd:name/gco:CharacterString">
            <dc:format><xsl:value-of select="."/></dc:format>
          </xsl:for-each>
        </xsl:for-each>
      </simpleDc>

      <dcTerms>
        <dcterms:modified><xsl:value-of select="gmd:dateStamp/gco:DateTime"/></dcterms:modified>

        <!-- DataIdentification - - - - - - - - - - - - - - - - - - - - - -->
        <xsl:for-each select="gmd:identificationInfo/gmd:MD_DataIdentification">

          <xsl:for-each select="gmd:citation/gmd:CI_Citation">
            <xsl:for-each select="gmd:date/gmd:CI_Date[gmd:dateType/gmd:CI_DateTypeCode/@codeListValue='creation']/gmd:date/gco:DateTime">
              <dcterms:created><xsl:value-of select="."/></dcterms:created>
            </xsl:for-each>

            <xsl:for-each select="gmd:date/gmd:CI_Date[gmd:dateType/gmd:CI_DateTypeCode/@codeListValue='publication']/gmd:date/gco:DateTime">
              <dcterms:created><xsl:value-of select="."/></dcterms:created>
            </xsl:for-each>
          </xsl:for-each>

          <!-- description -->
          <xsl:for-each select="gmd:abstract/gco:CharacterString">
            <dcterms:abstract><xsl:value-of select="."/></dcterms:abstract>
          </xsl:for-each>

          <!-- rights -->
          <xsl:for-each select="gmd:resourceConstraints/gmd:MD_LegalConstraints">
            <xsl:for-each select="*/gmd:MD_RestrictionCode/@codeListValue">
              <dcterms:accessRights><xsl:value-of select="."/></dcterms:accessRights>
            </xsl:for-each>

            <xsl:for-each select="gmd:otherConstraints/gco:CharacterString">
              <dcterms:accessRights><xsl:value-of select="."/></dcterms:accessRights>
            </xsl:for-each>
          </xsl:for-each>

          <!-- bounding box -->
          <xsl:for-each select="gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox">	
            <dcterms:spatial xsi:type="dcterms:Box">
              <xsl:value-of select="concat('northlimit=', gmd:northBoundLatitude/gco:Decimal, '; ')"/>
              <xsl:value-of select="concat('southlimit=', gmd:southBoundLatitude/gco:Decimal, '; ')"/>
              <xsl:value-of select="concat('eastlimit=' , gmd:eastBoundLongitude/gco:Decimal, '; ')"/>
              <xsl:value-of select="concat('westlimit=' , gmd:westBoundLongitude/gco:Decimal)"/>
            </dcterms:spatial>
          </xsl:for-each>

          <!-- temporal bounds -->
          <xsl:for-each select="gmd:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod">
            <dcterms:temporal>
              <xsl:value-of select="concat('Begin ', gml:beginPosition, '; ')"/>
              <xsl:value-of select="concat('End ' ,  gml:endPosition)"/>
            </dcterms:temporal>
          </xsl:for-each>

        </xsl:for-each>
      </dcTerms>
<!--
      <otherElements>
      </otherElements>
-->
    </metadata>
  </xsl:template>

  <xsl:template match="*">
    <xsl:apply-templates select="*"/>
  </xsl:template>
</xsl:stylesheet>''')

  try:
    transform = et.XSLT(xslDoc)
  except:
    print "unable to generate transform, exiting..."
    return

  print ""

  # client to interact with GMN
  client = d1_client.mnclient.MemberNodeClient(
                                GMN_URL,
                                cert_path=CERTIFICATE_FOR_CREATE,
                                key_path=CERTIFICATE_FOR_CREATE_KEY)

  # get the list of objects on the GMN
  try:
    objs = client.listObjects(
                     count=1,
                     objectFormat=RMAP_FORMAT_ID,
                     replicaStatus=False)
    tot = objs.total
    sleep(0.1)
    objs = client.listObjects(
                     count=tot,
                     objectFormat=RMAP_FORMAT_ID,
                     replicaStatus=False)
  except d1_common.types.exceptions.DataONEException as e:
    print "listObjects() failed with exception:"
    raise

  # generate a list of pid strings
  objStrings = [ obj.identifier.value() for obj in objs.objectInfo ]

  # for each fileID, get the latest resource map
  sleep(0.1)
  count = 0
  for fileID in fileIDs:
    count += 1
    
    # start at a higher number?
    if len(argv) > 1 and int(argv[1]) > 0 and count < int(argv[1]):
      continue
      
    print "record number: " + str(count)

    # get the ISO 19139 record
    try:
      sleep(0.2)
      fo = urlopen(GEO_URL + "?verb=GetRecord&metadataPrefix=iso19139&identifier=" + str(fileID))
    except:
      print "error opening " + str(fileID)
      print "continuing..."
      continue
    else:
      # ingest metadata & xslt transform to dcx, the metadata format used on the GMN
      isoDoc = et.parse(fo)

      dcxDoc = transform(isoDoc)
      dcxString = et.tostring(dcxDoc)
      dcxString = '<?xml version="1.0" encoding="UTF-8"?>' + dcxString
      #print dcxString

      # validate dcxDoc, assumes onedcx_v1.0.xsd and associated xsd
      # files are in same directory where this script is run:
      # dcmitype.xsd
      # dcterms.xsd <--- original, not LoC version
      # dc.xsd
      # onedcx_v1.0.xsd
      # xml.xsd

      dcxXsd = et.XMLSchema(et.parse("onedcx_v1.0.xsd"))
      if dcxXsd.validate(dcxDoc):

        # extract original ISO metadata from OAI-PMH wrapper to upload as data
        isoXML = et.tostring(isoDoc.find("//{http://www.isotc211.org/2005/gmd}MD_Metadata"))
        isoXML = '<?xml version="1.0" encoding="UTF-8"?>\n' + isoXML.replace("\n        ","\n")
        #print isoXML

        # the package will consist of dcx metadata, with pid
        # "dcx_" + fileID + "_" + version
        # ISO 19139 metadata stored as data, text/xml, with pid
        # "iso19139_" + fileID + "_" + version
        # data, and a resource map tying the two together, with pid
        # fileID + "_" + version

        # the following assumes we keep all versions of the resource map objects
        # so that we can walk the chain from the first one to the most recent.
        # when SIDs become available in DataONE API v2, we'll use those to get to
        # the most recent version, i.e. SID will equal fileID w/o version suffix.

        # the whole reason to walk the chain is to get the latest index (idx),
        # so that an update can have idx = idx + 1
        idx = 0
        packageJustCreated = 0
        while True:
          rmap = fileID + "_" + str(idx)

          if rmap in objStrings:
            idx += 1
            continue
          else:
            if idx==0: # initial package creation
              sleep(0.1)
              if not createInitialPackage(dcxString, isoXML, fileID, client):
                print "package creation failure for " + fileID + "_" + str(idx)
                print "halting; either there is a network problem (try running this script again),"
                print "and/or the package already exists (please investigate)..."
                return
              else:
                packageJustCreated = 1
                sleep(0.1)
                break
            else:
              idx -= 1
              break

        # check if update required: get the latest ISO 19139 data object, compare
        # with downloaded OAI-PMH version, and update package if different
        if not packageJustCreated:
          sleep(0.1)
          try:
            isoDO = client.get("iso19139_" + fileID + "_" + str(idx)).read()
          except:
            print "ISO metadata retrieval error for iso19139_" + fileID + "_" + str(idx)
            print "halting; probably a network problem (try running this script again)."
            return
          else:
            if isoDO != isoXML:
              print "changes in " + "iso19139_" + fileID + "_" + str(idx) + " detected,"
              print "updating package, new index is " +  "_" + str(idx+1)
              if not updatePackage(dcxString, isoXML, fileID, idx, client):
                print "package update failure for " + fileID + "_" + str(idx)
                print "halting; either there is a network problem (try running this script again),"
                print "and/or the package already exists (please investigate)..."
                return
            elif FORCE_UPDATE:
              print "update forced for " + "iso19139_" + fileID + "_" + str(idx)
              print "new index is " +  "_" + str(idx+1)
              if not updatePackage(dcxString, isoXML, fileID, idx, client):
                print "package update failure for " + fileID + "_" + str(idx)
                print "halting; either there is a network problem (try running this script again),"
                print "and/or the package already exists (please investigate)..."
                return
            else:
              print "no update required for " + fileID + "_" + str(idx)
              sleep(0.1)

      else:
        print str(fileID) + " did not validate for dcx, skipping..."
        sleep(0.1)

    print ""

  return
## end main()

def createInitialPackage(dcxString, isoXML, fileID, client):
  now = datetime.now()

  # create metadata object
  pids = ["dcx_" + fileID]
  print "creating metadata object " + pids[0] + "_0"
  sysMeta = create_sys_meta(
              pids[0],
              META_FORMAT_ID,
              0,
              len(dcxString),
              dataoneTypes.checksum(hashlib.sha1(dcxString).hexdigest()),
              now)
  pids[0] = pids[0] + "_0"

  try:
    sleep(0.1)
    client.create(pids[0], StringIO.StringIO(dcxString), sysMeta)
  except:
    print "creation of metadata object " + pids[0] + " failed"
    return False

  # create data object, the ISO 19139 metadata xml
  pids = pids + ["iso19139_" + fileID]
  print "creating data object " + pids[-1] + "_0"
  sysMeta = create_sys_meta(
              pids[-1],
              DATA_FORMAT_ID,
              0,
              len(isoXML),
              dataoneTypes.checksum(hashlib.sha1(isoXML).hexdigest()),
              now)
  pids[-1] = pids[-1] + "_0"

  try:
    sleep(0.1)
    client.create(pids[-1], StringIO.StringIO(isoXML), sysMeta)
  except:
    print "creation of data object " + pids[-1] + " failed"
    print "rolling back..."

    try:
      sleep(0.1)
      client.delete(pids[0])
      print "rollback deletion of metadata object " + pids[0] + " succeeded"
    except:

      try: # again
        sleep(0.1)
        client.delete(pids[0])
        print "rollback deletion of metadata object " + pids[0] + " succeeded"
      except:
        print "rollback deletion of metadata object " + pids[0] + " failed"
        print "manual intervention required to delete object."

    return False

  # create resource map
  pid = fileID + "_0"
  print "creating resource map " + pid
  rmapGenerator = d1_client.data_package.ResourceMapGenerator()
  rmap = rmapGenerator.simple_generate_resource_map(pid, pids[0], pids[1:])
  sysMeta = create_sys_meta(
              fileID,
              RMAP_FORMAT_ID,
              0,
              len(rmap),
              dataoneTypes.checksum(hashlib.sha1(rmap).hexdigest()),
              now)

  try:
    sleep(0.1)
    client.create(pid, StringIO.StringIO(rmap), sysMeta)
  except:
    print "creation of resource map " + pid + " failed"
    print "rolling back..."

    try:
      sleep(0.1)
      client.delete(pids[-1])
      print "rollback deletion of data object " + pids[-1] + " succeeded"
    except:

      try: # again
        sleep(0.1)
        client.delete(pids[-1])
        print "rollback deletion of data object " + pids[-1] + " succeeded"
      except:
        print "rollback deletion of data object " + pids[-1] + " failed"
        print "manual intervention required to delete object."

    try:
      sleep(0.1)
      client.delete(pids[0])
      print "rollback deletion of metadata object " + pids[0] + " succeeded"
    except:

      try: # again
        sleep(0.1)
        client.delete(pids[0])
        print "rollback deletion of metadata object " + pids[0] + " succeeded"
      except:
        print "rollback deletion of metadata object " + pids[0] + " failed"
        print "manual intervention required to delete object."

    return False

  # creation of resource map succeeded
  else:
    print "package creation for " + pid + " successful."

  return True


def create_sys_meta(pid, format_id, idx, size, sha1, when):
  sysMeta                         = dataoneTypes.systemMetadata()
  sysMeta.serialVersion           = idx
  sysMeta.identifier              = pid + "_" + str(idx)
  sysMeta.formatId                = format_id
  sysMeta.size                    = size
  sysMeta.rightsHolder            = SYSMETA_RIGHTSHOLDER
  sysMeta.checksum                = sha1
  sysMeta.checksum.algorithm      = 'SHA-1'
  sysMeta.dateUploaded            = when
  sysMeta.dateSysMetadataModified = when
  sysMeta.accessPolicy            = generate_public_access_policy()
  sysMeta.replicationPolicy       = generate_replication_policy()

  return sysMeta


def generate_public_access_policy():
  accessPolicy = dataoneTypes.accessPolicy()
  accessRule = dataoneTypes.AccessRule()
  accessRule.subject.append(d1_common.const.SUBJECT_PUBLIC)
  permission = dataoneTypes.Permission('read')
  accessRule.permission.append(permission)
  accessPolicy.append(accessRule)

  return accessPolicy


def generate_replication_policy():
  replicationPolicy = dataoneTypes.replicationPolicy()
  replicationPolicy.replicationAllowed = True
  replicationPolicy.numberReplicas     = d1_common.const.DEFAULT_NUMBER_OF_REPLICAS
  
  return replicationPolicy


def updatePackage(dcxString, isoXML, fileID, idx, client):
  now = datetime.now()

  # update metadata object
  pids = ["dcx_" + fileID]
  print "updating: " + pids[0] + "_" + str(idx)
  sysMeta = create_sys_meta(
              pids[0],
              META_FORMAT_ID,
              idx+1,
              len(dcxString),
              dataoneTypes.checksum(hashlib.sha1(dcxString).hexdigest()),
              now)
  oldpid  = pids[0] + "_" + str(idx)
  pids[0] = pids[0] + "_" + str(idx+1)

  try:
    sleep(0.1)
    client.update(oldpid, StringIO.StringIO(dcxString), pids[0], sysMeta)
  except d1_common.types.exceptions.DataONEException as e:
    print "update of " + oldpid + " failed with exception:"
    raise
  else:
    print "update of " + oldpid + " succeeded"

  # update data object, the ISO 19139 metadata xml
  pids = pids + ["iso19139_" + fileID]
  print "updating: " + pids[-1] + "_" + str(idx)
  sysMeta = create_sys_meta(
              pids[-1],
              DATA_FORMAT_ID,
              idx+1,
              len(isoXML),
              dataoneTypes.checksum(hashlib.sha1(isoXML).hexdigest()),
              now)
  oldpid   = pids[-1] + "_" + str(idx)
  pids[-1] = pids[-1] + "_" + str(idx+1)

  try:
    sleep(0.1)
    client.update(oldpid, StringIO.StringIO(isoXML), pids[-1], sysMeta)
  except d1_common.types.exceptions.DataONEException as e:
    print "manual intervention required due to inconsistent package state:"
    print pids[0] + "has obsoleted dcx_" + fileID + str(idx) + ", but"
    print "update of " + oldpid + " failed with exception:"
    raise
  else:
    print "update of " + oldpid + " succeeded"

  # update resource map
  oldpid = fileID + "_" + str(idx)
  newpid = fileID + "_" + str(idx+1)
  print "updating: " + oldpid
  rmapGenerator = d1_client.data_package.ResourceMapGenerator()
  rmap = rmapGenerator.simple_generate_resource_map(newpid, pids[0], pids[1:])
  sysMeta = create_sys_meta(
              fileID,
              RMAP_FORMAT_ID,
              idx+1,
              len(rmap),
              dataoneTypes.checksum(hashlib.sha1(rmap).hexdigest()),
              now)

  try:
    sleep(0.1)
    client.update(oldpid, StringIO.StringIO(rmap), newpid, sysMeta)
  except d1_common.types.exceptions.DataONEException as e:
    print "manual intervention required due to inconsistent package state:"
    print pids[0]  + "has obsoleted dcx_"      + fileID + str(idx) + ", and"
    print pids[-1] + "has obsoleted iso19139_" + fileID + str(idx) + ", but"
    print "update of " + oldpid + " failed with exception:"
    raise
  else:
    print "update of " + oldpid + " succeeded"
    print "package update for " + oldpid + " successful."

  return True


if __name__ == '__main__':
  main()
