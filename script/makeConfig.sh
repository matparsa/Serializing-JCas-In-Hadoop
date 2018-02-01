#!/bin/bash
echo "##############################################################################"
echo "##                                                                          ##"
echo "##       Making configurtion file for uima-jCasSerializer-hadoop            ##"
echo "##                                                                          ##"
echo "##############################################################################"

echo "Do you have a runnable jar (includes all the required library) of your app? [y/n]"

read hasJar;
if [ "$hasJar" != "y" ]; then
  echo " -you can read the readme to see how you can make it."
  exit 1
fi

echo "Insert the path of your jar:"
read jarPath
echo "Insert name of class together with the package name that you prepared (readme for more information):"
read className
echo "Insert name of the method that get your main descriptor path return an AnalysisEngine(readme for more information):"
read analysisEngineMethod

echo "Do you want to Serialize or Deserialize your input? [s/d]"
read method

if [ "$method" == "d" ]; then
  echo "Insert name of the method that get a JCas and return String(readme for more information):"
  read proccesMethod
else
  proccesMethod='non'
fi

# Uima cant use the embeded resources in jar and also some pipelines read the model from embeded resources ; therefore, I embeded the resources into the jar and also compress them to send them to the cluster later.
echo "Insert the path of your UIMA-Resources with your path structure:"
read resources
currentDir=$(pwd)
cd $resources
zip -r $currentDir/ArchivedResources.zip ./
# echo "Do you want to embed the resourses to the jar? [y/n]"
# read embed
# if [ "$embed" == "y" ]; then
#   echo "Insert the path of your UIMA-Resources without path structure:"
#   echo "Directory name should be Resources!"
#   echo "enter [same] if it's the same as last path:"
#   read embResource
#   if [ "$embResource" == "same" ]; then
# 	$embResource=$resources
#   fi
#   cd $embResource/..
#   jar -uvf $jarPath ./Resources
# fi
cd $currentDir


echo "Insert the input path:"
read input_path

echo "Insert the output path:"
read output_path

#make config file
echo jarPath:$jarPath className:$className analysisEngineMethod:$analysisEngineMethod proccesMethod:$proccesMethod describtorsFilePath:$resources archivedDescribtorsFilePath:$currentDir/ArchivedResources.zip input_path:$input_path output_path:$output_path > uima_hadoop_serializer.config

 
echo -ne '#####                     (33%)\r'
sleep 1
echo -ne '#############             (66%)\r'
sleep 1
echo -ne '#######################   (100%)\r'
echo -ne '\n'
clear
echo "##############################################################################"
echo '  configuration file has been succesfully created ! '
echo "##############################################################################"
sleep 1
echo 'You can find the  here:' $currentDir/uima_hadoop_serializer.config
echo ' If you use some external projects or library you need to add them to the uima-hadoop project and again make a jar! '
echo ''

